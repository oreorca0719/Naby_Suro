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
# 데미지 지수 = 주스탯풀 × 공격력풀 × 보스뎀배율 × 크리배율.
# 실제 메이플 데미지처럼 각 요소를 곱연산으로 결합한다(합연산 근사 아님).
ATK_TO_STAT = 3.0        # 공격력/마력 flat 1 = 주스탯 3 (스탯공격력 환산)
# 주스탯%/올스탯% : 주스탯 flat 풀에 곱연산
# 공격력%/마력%   : 공격력 flat 풀에 곱연산
# 보공%+데미지%   : (1 + 합/100) 배율
# 크리티컬 데미지% : (1 + 크뎀%/100) 배율 (크확 100% 가정)
DMG_SCALE = 100_000.0    # 지수 자릿수 정규화(상대 비교용, 기존 점수대와 유사)

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


# 옵션은 아래 6개 버킷으로 분해해 담고, _score_comp 에서 곱연산 결합한다.
#   flat_main : 주스탯·올스탯 flat  → 주스탯 풀
#   main_pct  : 주스탯%·올스탯%     → 주스탯 풀에 곱연산
#   flat_atk  : 공격력·마력 flat    → 공격력 풀(×ATK_TO_STAT)
#   atk_pct   : 공격력%·마력%       → 공격력 풀에 곱연산
#   boss      : 보공%·데미지%       → 보스뎀 배율
#   crit      : 크리티컬 데미지%    → 크리 배율
# 최종스탯 기반 캐릭터별 가중치는 '현재 착용' 상태에 종속돼 최강 세팅 기준을
# 깨뜨리므로(빠민 사례) 쓰지 않고, 장비 옵션만으로 계산한다.


def _empty_comp() -> dict:
    """옵션 컴포넌트 누적기. 주스탯% 는 마지막에 flat 풀에 곱연산하기 위해
    다른 옵션과 분리해서 담는다."""
    return {"flat_main": 0.0, "flat_atk": 0.0, "boss": 0.0,
            "crit": 0.0, "atk_pct": 0.0, "main_pct": 0.0, "count": 0}


def _add_option(comp: dict, name: str, v: float, is_pct: bool, main_stat: str) -> None:
    """옵션 하나(이름+수치)를 컴포넌트 누적기에 더한다. 잠재·세트 공용."""
    atk_label = _atk_label(main_stat)
    if is_pct:
        if name == main_stat or name == "올스탯":
            comp["main_pct"] += v          # 주스탯 풀에 곱연산될 %
        elif name == atk_label:
            comp["atk_pct"] += v
        elif name in ("보스 몬스터 데미지", "데미지"):
            comp["boss"] += v
        elif name == "크리티컬 데미지":
            comp["crit"] += v
    else:  # flat — 올스탯 flat 은 주스탯 풀에 그대로 더한다
        if name == main_stat or name == "올스탯":
            comp["flat_main"] += v
        elif name == atk_label:
            comp["flat_atk"] += v


def _add_option_line(comp: dict, raw: str, main_stat: str) -> None:
    """'공격력 +40' / '보스 몬스터 데미지 +10%' 한 토큰을 컴포넌트에 누적."""
    raw = raw.strip()
    if not raw:
        return
    m = _PCT.match(raw)
    if m:
        _add_option(comp, m.group("name").strip(), int(m.group("v")), True, main_stat)
        return
    m = _FLAT.match(raw)
    if m:
        _add_option(comp, m.group("name").strip(), int(m.group("v")), False, main_stat)


def _score_comp(comp: dict) -> float:
    """컴포넌트 누적기를 데미지 지수로. 실제 데미지처럼 곱연산으로 결합한다.

        데미지지수 = 주스탯풀 × 공격력풀 × 보스뎀배율 × 크리배율 / DMG_SCALE
          주스탯풀   = flat_main × (1 + 주스탯%/100)
          공격력풀   = flat_atk × ATK_TO_STAT × (1 + 공격력%/100)
          보스뎀배율 = 1 + (보공% + 데미지%)/100
          크리배율   = 1 + 크뎀%/100   (크확 100% 가정)

    보공·크뎀은 서로, 그리고 주스탯·공격력과도 곱해지므로 세트 붕괴 같은
    데미지 계열 하향이 폭발적으로(정확하게) 반영된다.
    """
    stat_pool = comp["flat_main"] * (1.0 + comp["main_pct"] / 100.0)
    atk_pool = comp["flat_atk"] * ATK_TO_STAT * (1.0 + comp["atk_pct"] / 100.0)
    boss_mult = 1.0 + comp["boss"] / 100.0
    crit_mult = 1.0 + comp["crit"] / 100.0
    return stat_pool * atk_pool * boss_mult * crit_mult / DMG_SCALE


def preset_components(items: list[dict] | None, main_stat: str, level: int) -> dict | None:
    """프리셋 하나를 컴포넌트로 분해.

    item_total_option 은 기본/추옵/주문서/스타포스를 합산한 값이지만 익셉셔널은
    포함하지 않아 따로 더한다. 잠재는 %옵션이라 문자열을 파싱한다.
    """
    if not items:
        return None
    comp = _empty_comp()
    stat_key = main_stat.lower()
    atk_key = _atk_key(main_stat)
    for it in items:
        if it.get("item_equipment_slot") in EXCLUDED_SLOTS:
            continue
        comp["count"] += 1
        total = it.get("item_total_option") or {}
        exc = it.get("item_exceptional_option") or {}
        comp["flat_main"] += (int(total.get(stat_key) or 0) + int(exc.get(stat_key) or 0)
                              + int(total.get("all_stat") or 0))
        comp["flat_atk"] += int(total.get(atk_key) or 0) + int(exc.get(atk_key) or 0)
        comp["boss"] += int(total.get("boss_damage") or 0) + int(total.get("damage") or 0)
        for key in _POT_KEYS:
            raw = (it.get(key) or "").strip()
            if not raw:
                continue
            m = _PER_LEVEL.match(raw)
            if m:  # 레벨당 주스탯 — flat 으로 취급(%무관)
                name, per, v = m.group("name"), int(m.group("per")), int(m.group("v"))
                if name == main_stat or name == "올스탯":
                    comp["flat_main"] += (level // per) * v
                continue
            _add_option_line(comp, raw, main_stat)
    return comp


def _add_option_string(comp: dict, s: str, main_stat: str) -> None:
    """콤마/줄바꿈으로 이어진 옵션 문자열을 토큰별로 컴포넌트에 누적."""
    for tok in re.split(r"[,\n]", s or ""):
        _add_option_line(comp, tok, main_stat)


# ── 세트 정의 (최강 프리셋 기준 세트효과 계산용) ──────────────────────
# 넥슨 set-effect API 는 '현재 착용' 세트만 준다. 최강 프리셋을 안 입고 접속
# 종료하면 세트가 흔들려 노이즈가 크므로(썽이 사례), 최강 프리셋의 아이템
# 이름으로 세트 개수를 직접 세어 착용상태와 무관하게 계산한다.
#
# 매핑에 없는 세트(아케인셰이드 등 방어구 세트)는 프리셋 간 동일해 주간
# 변화가 없으므로 탐지에 영향을 주지 않는다. 필요 시 아래에 추가하면 된다.
SET_MEMBERS: dict[str, set[str]] = {
    "여명의 보스 세트": {
        "트와일라이트 마크", "에스텔라 이어링",
        "여명의 가디언 엔젤 링", "데이브레이크 펜던트",
    },
    "칠흑의 보스 세트": {
        "루즈 컨트롤 머신 마크", "마력이 깃든 안대", "컴플리트 언더컨트롤",
        "몽환의 벨트", "고통의 근원", "창세의 뱃지", "커맨더 포스 이어링",
        "거대한 공포",
    },
}
# 변형 이름(택1) 부분일치 멤버: (세트, 슬롯, 포함문구)
SET_MEMBERS_CONTAINS: list[tuple[str, str, str]] = [
    ("칠흑의 보스 세트", "포켓 아이템", "저주받은"),   # 저주받은 마도서 계열
    ("칠흑의 보스 세트", "엠블렘", "미트라의 분노"),
]
# 세트 단계별(2~) 효과. 게임 UI '보스 몬스터 공격 시 데미지'는 API 표기
# '보스 몬스터 데미지'로 통일. 방어율 무시·최대HP·방어력은 환산 제외(자동 무시).
SET_TIERS: dict[str, dict[int, str]] = {
    "여명의 보스 세트": {
        2: "올스탯 +10, 공격력 +10, 마력 +10, 보스 몬스터 데미지 +10%",
        3: "올스탯 +10, 공격력 +10, 마력 +10",
        4: "올스탯 +10, 공격력 +10, 마력 +10",
    },
    "칠흑의 보스 세트": {
        2: "올스탯 +10, 공격력 +10, 마력 +10, 보스 몬스터 데미지 +10%",
        3: "올스탯 +10, 공격력 +10, 마력 +10",
        4: "올스탯 +15, 공격력 +15, 마력 +15, 크리티컬 데미지 +5%",
        5: "올스탯 +15, 공격력 +15, 마력 +15, 보스 몬스터 데미지 +10%",
        6: "올스탯 +15, 공격력 +15, 마력 +15",
        7: "올스탯 +15, 공격력 +15, 마력 +15, 크리티컬 데미지 +5%",
        8: "올스탯 +15, 공격력 +15, 마력 +15, 보스 몬스터 데미지 +10%",
        9: "올스탯 +15, 공격력 +15, 마력 +15, 크리티컬 데미지 +5%",
        10: "올스탯 +20, 공격력 +20, 마력 +20, 보스 몬스터 데미지 +10%",
    },
}


def count_sets(items: list[dict] | None) -> dict[str, int]:
    """프리셋 아이템 이름으로 매핑된 세트별 착용 개수를 센다."""
    counts: dict[str, int] = {}
    if not items:
        return counts
    for it in items:
        slot = it.get("item_equipment_slot")
        if slot in EXCLUDED_SLOTS:
            continue
        name = it.get("item_name", "")
        for set_name, members in SET_MEMBERS.items():
            if name in members:
                counts[set_name] = counts.get(set_name, 0) + 1
        for set_name, exp_slot, needle in SET_MEMBERS_CONTAINS:
            if slot == exp_slot and needle in name:
                counts[set_name] = counts.get(set_name, 0) + 1
    return counts


def preset_set_components(items: list[dict] | None, main_stat: str) -> dict:
    """최강 프리셋 아이템 이름으로 센 세트효과를 컴포넌트로.

    착용 상태와 무관하게 그 프리셋 자체의 세트효과를 계산한다.
    """
    comp = _empty_comp()
    for set_name, cnt in count_sets(items).items():
        tiers = SET_TIERS.get(set_name, {})
        for tier, opt in tiers.items():
            if tier <= cnt:
                _add_option_string(comp, opt, main_stat)
    return comp


def set_components(set_effect: dict | None, main_stat: str) -> dict:
    """(참고용) 현재 착용 세트효과 — 넥슨 API 응답 기준. 검증에만 쓴다."""
    comp = _empty_comp()
    if not set_effect:
        return comp
    for s in set_effect.get("set_effect", []):
        cnt = int(s.get("total_set_count") or 0)
        if cnt < 2:
            continue
        for info in s.get("set_effect_info", []):
            if int(info.get("set_count") or 0) <= cnt:
                _add_option_string(comp, info.get("set_option") or "", main_stat)
    return comp


def _merge_comp(a: dict, b: dict) -> dict:
    out = _empty_comp()
    for k in out:
        out[k] = a[k] + b[k]
    return out


def character_spec(basic: dict, stat: dict, equipment: dict) -> dict:
    """캐릭터의 스펙 스냅샷. 가장 강한 프리셋을 그 사람의 보스 세팅으로 본다.

    최종 점수 = 최강 프리셋(아이템 + 그 프리셋의 세트효과)의 주스탯 환산 총합.
    주스탯%는 아이템·세트의 주스탯 flat 풀에 곱연산으로 적용한다(게임과 동일).
    세트효과는 넥슨 API(착용 기준) 대신 최강 프리셋 아이템 이름으로 직접 세어
    계산하므로, 접속 종료 시 어떤 프리셋을 입었든 값이 흔들리지 않는다.

    Args:
        basic:      /character/basic 응답 (레벨)
        stat:       /character/stat 응답 (주스탯 판별)
        equipment:  /character/item-equipment 응답 (프리셋 3개)
    """
    level = int(basic.get("character_level") or 0)
    main_stat = detect_main_stat(stat.get("final_stat") or [])

    presets = {}
    preset_items = {}
    for n in (1, 2, 3):
        items = equipment.get(f"item_equipment_preset_{n}")
        c = preset_components(items, main_stat, level)
        if c:
            presets[n] = c
            preset_items[n] = items

    if not presets:
        return {
            "level": level, "main_stat": main_stat,
            "best_preset": None, "score": 0,
            "item_score": 0, "set_score": 0, "presets": {},
        }

    best_no = max(presets, key=lambda k: _score_comp(presets[k]))
    best = presets[best_no]
    # 세트효과는 최강 프리셋 아이템 이름으로 직접 계산(착용상태 무관)
    setc = preset_set_components(preset_items[best_no], main_stat)
    item_score = round(_score_comp(best), 1)
    combined = _merge_comp(best, setc)          # 세트를 주스탯 풀에 합쳐 곱연산
    total = round(_score_comp(combined), 1)
    return {
        "character_class": basic.get("character_class"),
        "level": level,
        "main_stat": main_stat,
        "best_preset": best_no,
        "current_preset": equipment.get("preset_no"),
        "score": total,
        "item_score": item_score,
        "set_score": round(total - item_score, 1),  # 세트의 한계 기여(곱연산 상호작용 포함)
        "main_pct": round(combined["main_pct"], 1),
        "flat_main": round(combined["flat_main"], 1),
        "item_count": best["count"],
        "presets": {str(k): round(_score_comp(v), 1) for k, v in presets.items()},
    }
