"""장비 스펙 환산 — 스펙 다운 탐지용.

메이플에서 스펙을 낮추는 방법은 사실상 "약한 아이템을 착용하는 것" 뿐이다.
따라서 보스 전용 프리셋(가장 센 조합)의 장비 옵션 총합이 하락했는지 보면
의도적인 스펙 다운을 감지할 수 있다.

전투력 API 값은 쓰지 않는다. 조회 시점에 사냥 세팅을 착용 중이면 실제보다
낮게 찍히고(2배 이상 차이), 사용자가 어떤 상태로 접속을 종료했는지에 종속되어
같은 스펙이어도 주마다 값이 달라지기 때문이다. 프리셋은 종료 상태와 무관하게
3개 모두 조회되므로 일관된 비교가 가능하다.

환산 계수 (주스탯 1 기준):
    공격력/마력 1              -> 3
    주스탯 1%                  -> 10
    공격력/마력 1%             -> 10 * 3
    보공% + 데미지% ("보총뎀")  -> 11
    크리티컬 데미지 1%          -> 3.5
"""
from __future__ import annotations

import re

# ── 환산 계수 ─────────────────────────────────────────────────────
ATK_TO_STAT = 3.0        # 공격력/마력 1 = 주스탯 3
PCT_TO_FLAT = 10.0       # 1% = 해당 스탯 10
BOSS_DMG_TO_STAT = 11.0  # 보공/데미지 1% = 주스탯 11
CRIT_DMG_TO_STAT = 3.5   # 크리티컬 데미지 1% = 주스탯 3.5

MAIN_STATS = ("STR", "DEX", "INT", "LUK")
# 마력을 쓰는 주스탯. 그 외는 공격력을 본다.
MAGIC_STAT = "INT"

# 실제 착용이 아닌 예비 슬롯. 환산에서 제외한다.
EXCLUDED_SLOTS = {"예비 특수 반지"}

_POT_KEYS = (
    "potential_option_1", "potential_option_2", "potential_option_3",
    "additional_potential_option_1", "additional_potential_option_2",
    "additional_potential_option_3",
)

# "STR +9%", "공격력 +12%", "보스 몬스터 데미지 +45%"
_PCT = re.compile(r"^(?P<name>.+?)\s*\+(?P<v>\d+)%$")
# "캐릭터 기준 9레벨 당 STR +2"
_PER_LEVEL = re.compile(r"^캐릭터 기준 (?P<per>\d+)레벨 당 (?P<name>\S+)\s*\+(?P<v>\d+)$")
# "STR +19", "공격력 +10"
_FLAT = re.compile(r"^(?P<name>.+?)\s*\+(?P<v>\d+)$")


def detect_main_stat(final_stat: list[dict]) -> str:
    """스탯창에서 가장 높은 값을 갖는 스탯을 주스탯으로 본다."""
    d = {x["stat_name"]: x["stat_value"] for x in final_stat}
    best, best_v = "STR", -1.0
    for s in MAIN_STATS:
        try:
            v = float(d.get(s, 0))
        except (TypeError, ValueError):
            v = 0.0
        if v > best_v:
            best, best_v = s, v
    return best


def _atk_key(main_stat: str) -> str:
    return "magic_power" if main_stat == MAGIC_STAT else "attack_power"


def _atk_label(main_stat: str) -> str:
    return "마력" if main_stat == MAGIC_STAT else "공격력"


def _potential_score(item: dict, main_stat: str, level: int) -> float:
    """잠재·에디셔널 잠재 옵션을 주스탯 환산값으로."""
    atk_label = _atk_label(main_stat)
    score = 0.0
    for key in _POT_KEYS:
        raw = (item.get(key) or "").strip()
        if not raw:
            continue

        m = _PER_LEVEL.match(raw)
        if m:  # 레벨 비례 옵션 — 캐릭터 레벨로 실효값 계산
            name, per, v = m.group("name"), int(m.group("per")), int(m.group("v"))
            if name == main_stat or name == "올스탯":
                score += (level // per) * v
            continue

        m = _PCT.match(raw)
        if m:
            name, v = m.group("name").strip(), int(m.group("v"))
            if name == main_stat or name == "올스탯":
                score += v * PCT_TO_FLAT
            elif name == atk_label:
                score += v * PCT_TO_FLAT * ATK_TO_STAT
            elif name in ("보스 몬스터 데미지", "데미지"):
                score += v * BOSS_DMG_TO_STAT
            elif name == "크리티컬 데미지":
                score += v * CRIT_DMG_TO_STAT
            continue

        m = _FLAT.match(raw)
        if m:
            name, v = m.group("name").strip(), int(m.group("v"))
            if name == main_stat:
                score += v
            elif name == atk_label:
                score += v * ATK_TO_STAT
    return score


def preset_score(items: list[dict] | None, main_stat: str, level: int) -> dict | None:
    """프리셋 하나의 환산 점수.

    item_total_option 은 기본/추옵/주문서/스타포스를 합산한 값이지만
    익셉셔널은 포함하지 않는다(실측 확인). 그래서 따로 더한다.
    잠재는 %옵션이라 total 에 없으므로 문자열을 파싱해 환산한다.
    """
    if not items:
        return None

    stat_key = main_stat.lower()
    atk_key = _atk_key(main_stat)
    flat_stat = flat_atk = 0
    pot = 0.0
    counted = 0

    for it in items:
        if it.get("item_equipment_slot") in EXCLUDED_SLOTS:
            continue
        counted += 1
        total = it.get("item_total_option") or {}
        exc = it.get("item_exceptional_option") or {}
        flat_stat += int(total.get(stat_key) or 0) + int(exc.get(stat_key) or 0)
        flat_atk += int(total.get(atk_key) or 0) + int(exc.get(atk_key) or 0)
        pot += _potential_score(it, main_stat, level)

    score = flat_stat + flat_atk * ATK_TO_STAT + pot
    return {
        "score": round(score, 1),
        "flat_stat": flat_stat,
        "flat_atk": flat_atk,
        "potential": round(pot, 1),
        "item_count": counted,
    }


def character_spec(basic: dict, stat: dict, equipment: dict) -> dict:
    """캐릭터의 스펙 스냅샷. 가장 강한 프리셋을 그 사람의 보스 세팅으로 본다.

    Args:
        basic:     /character/basic 응답 (레벨)
        stat:      /character/stat 응답 (주스탯 판별)
        equipment: /character/item-equipment 응답 (프리셋 3개)
    """
    level = int(basic.get("character_level") or 0)
    main_stat = detect_main_stat(stat.get("final_stat") or [])

    presets = {}
    for n in (1, 2, 3):
        r = preset_score(equipment.get(f"item_equipment_preset_{n}"), main_stat, level)
        if r:
            presets[n] = r

    if not presets:
        return {
            "level": level, "main_stat": main_stat,
            "best_preset": None, "score": 0, "presets": {},
        }

    best_no = max(presets, key=lambda k: presets[k]["score"])
    best = presets[best_no]
    return {
        "character_class": basic.get("character_class"),
        "level": level,
        "main_stat": main_stat,
        "best_preset": best_no,
        "current_preset": equipment.get("preset_no"),
        "score": best["score"],
        "flat_stat": best["flat_stat"],
        "flat_atk": best["flat_atk"],
        "potential": best["potential"],
        "item_count": best["item_count"],
        "presets": {str(k): v["score"] for k, v in presets.items()},
    }
