"""규칙 엔진 — 본캐 환산 / 철자 정규화 / 직업 백필 / 중복 제거.

파이프라인의 심장. 결정론적으로 처리할 수 있는 것만 확정하고,
확신할 수 없는 것은 절대 추측하지 않고 ambiguous 로 올려 사람에게 넘긴다.
("정답만 골라 담기"의 유일한 보장 장치)

괄호 규칙 (인게임 표기):
    A       (괄호 없음)  -> 본캐 A, 보이는 직업 그대로
    A(A)    (안 == 밖)   -> 본캐 A, 그대로 (본캐 접속 중)
    A(B)    (안 != 밖)   -> 본캐 B, 직업은 B의 것으로 백필
                            (행에 보이는 직업은 접속 중인 부캐 것이므로)
"""
from __future__ import annotations

import json
import re
import unicodedata
from collections import Counter

from ..config import DATA_DIR

_BRACKET = re.compile(r"^(?P<outer>[^()]+)\((?P<inner>[^()]*)\)$")
# 게임이 괄호 안을 잘라낸 형태: A(..), A(…), A( )
_TRUNCATED = re.compile(r"^[.…\s]*$")


def _nfc(s: str) -> str:
    return unicodedata.normalize("NFC", (s or "").strip())


def _load_normalize() -> dict:
    path = DATA_DIR / "normalize.json"
    if not path.exists():
        return {"job_corrections": {}, "nickname_corrections": {}, "renames": {}}
    return json.loads(path.read_text(encoding="utf-8"))


def _split_bracket(raw_name: str) -> tuple[str, str | None, bool]:
    """닉네임 원문을 (바깥, 괄호안, 잘림여부)로 분해."""
    m = _BRACKET.match(raw_name)
    if not m:
        return raw_name, None, False
    outer = m.group("outer").strip()
    inner = m.group("inner").strip()
    if _TRUNCATED.match(inner):      # A(..) — 게임이 잘라서 못 읽음
        return outer, None, True
    return outer, inner, False


def resolve(
    raw_rows: list[dict],
    roster: list[str],
    prev_week_rows: list[dict] | None = None,
) -> dict:
    """OCR 원본 행들을 확정 데이터로 변환한다.

    Args:
        raw_rows: Claude 가 이미지에서 읽은 원본.
                  [{"name": "A(B)", "job": "비숍", "score": 43030}, ...]
                  name 은 괄호를 포함한 화면 표기 그대로여야 한다.
        roster:   NEXON 길드 명단 (철자 정답지, 부캐 그룹 인접 순서)
        prev_week_rows: 직전 주차 확정 데이터 (직업 백필용)
                  [{"name": "...", "job": "...", "score": 123}, ...]

    Returns:
        {
          "resolved":  [{"name","job","score","source_name","note"}, ...],
          "ambiguous": [{"reason","raw","candidates","detail"}, ...],
          "stats": {...}
        }
    """
    norm = _load_normalize()
    job_fix: dict = norm.get("job_corrections", {})
    nick_fix: dict = {k: v for k, v in norm.get("nickname_corrections", {}).items()
                      if not k.startswith("_")}
    renames: dict = {k: v for k, v in norm.get("renames", {}).items()
                     if not k.startswith("_")}

    roster_set = {_nfc(n) for n in roster}
    roster_index = {_nfc(n): i for i, n in enumerate(roster)}
    prev_job = {_nfc(r["name"]): r.get("job", "") for r in (prev_week_rows or [])}

    resolved: list[dict] = []
    ambiguous: list[dict] = []

    def canon_ingame(name: str) -> str:
        """오독만 교정한 '인게임 현재 닉'. 길드 명단 대조는 이 값으로 한다."""
        n = _nfc(name)
        return nick_fix.get(n, n)

    def to_record_name(ingame: str) -> str:
        """기록용 닉. 개명한 회원은 히스토리 연속성을 위해 옛 닉으로 남긴다.

        주의: 개명 적용은 명단 대조를 통과한 '뒤'에 해야 한다.
        (명단에는 인게임 현재 닉이 들어있으므로 순서가 바뀌면 정상 회원이 오탐된다)
        """
        return renames.get(ingame, ingame)

    for row in raw_rows:
        raw_name = _nfc(row.get("name", ""))
        raw_job = _nfc(row.get("job", ""))
        score = row.get("score", 0)
        try:
            score = int(str(score).replace(",", "").strip() or 0)
        except ValueError:
            ambiguous.append({
                "reason": "점수 파싱 실패",
                "raw": row,
                "candidates": [],
                "detail": f"점수를 숫자로 읽을 수 없습니다: {row.get('score')!r}",
            })
            continue

        if not raw_name:
            ambiguous.append({
                "reason": "닉네임 없음", "raw": row, "candidates": [],
                "detail": "닉네임이 비어 있습니다.",
            })
            continue

        outer, inner, truncated = _split_bracket(raw_name)
        job = job_fix.get(raw_job, raw_job)
        note = ""

        if truncated:
            # A(..) — 괄호 안이 잘림. 명단 인접성으로 후보만 제시하고 확정하지 않는다.
            o = canon_ingame(outer)
            cands: list[str] = []
            if o in roster_index:
                i = roster_index[o]
                lo, hi = max(0, i - 3), min(len(roster), i + 4)
                cands = [roster[j] for j in range(lo, hi) if roster[j] != roster[i]]
            ambiguous.append({
                "reason": "괄호 안 본캐 잘림",
                "raw": row,
                "candidates": cands,
                "detail": (f"'{outer}' 는 부캐 접속 행으로 보이나 본캐가 '..' 로 잘렸습니다. "
                           f"명단 인접 후보를 확인하거나 크롭 확대 재판독이 필요합니다."),
            })
            continue

        # 1) 인게임 현재 닉 확정 (개명 적용 전) — 명단 대조용
        if inner is None:
            ingame = canon_ingame(outer)                   # 괄호 없음
        elif _nfc(inner) == _nfc(outer):
            ingame = canon_ingame(outer)                   # A(A) 본캐 접속
        else:
            ingame = canon_ingame(inner)                   # A(B) -> 본캐는 B
            note = f"부캐 접속({outer}) → 본캐 환산"

        # 2) 길드 명단 대조 (인게임 닉 기준)
        if ingame not in roster_set:
            ambiguous.append({
                "reason": "명단에 없는 닉네임",
                "raw": row,
                "candidates": [],
                "detail": (f"'{ingame}' 은(는) 이번 주 길드 명단에 없습니다. "
                           f"OCR 오독이거나 개명 직후일 수 있어 확인이 필요합니다."),
            })
            continue

        # 3) 기록용 닉 (개명 회원은 옛 닉으로 — 히스토리 연속성)
        main = to_record_name(ingame)
        if main != ingame:
            note = (note + " · " if note else "") + f"개명 반영({ingame}→{main})"

        # 4) 부캐 접속행이면 직업 백필 (화면 직업은 부캐의 것이므로)
        if inner is not None and _nfc(inner) != _nfc(outer):
            backfilled = prev_job.get(main, "")
            if backfilled:
                job = job_fix.get(backfilled, backfilled)
                note += " · 직업 백필"
            else:
                ambiguous.append({
                    "reason": "본캐 직업 백필 불가",
                    "raw": row,
                    "candidates": [],
                    "detail": (f"'{outer}' 는 부캐 접속이라 화면 직업({raw_job})은 부캐의 것입니다. "
                               f"본캐 '{main}' 의 직전 주차 기록이 없어 직업을 확정할 수 없습니다."),
                })
                continue

        resolved.append({
            "name": main,
            "job": job,
            "score": score,
            "source_name": raw_name,
            "note": note,
        })

    # 중복 정리 — 같은 본캐가 여러 행에 나온 경우
    #   길드 정책상 길드 컨텐츠는 본 캐릭터 1개만 참여 가능하므로,
    #   한 회원이 서로 다른 점수를 갖는 상황은 정상적으로 발생할 수 없다.
    #   → 합산·최댓값 같은 임의 병합을 하지 않고 사람에게 확인받는다.
    dedup: dict[str, dict] = {}
    dup_conflicts: list[dict] = []
    for r in resolved:
        prev = dedup.get(r["name"])
        if prev is None:
            dedup[r["name"]] = r
        elif prev["score"] == r["score"]:
            continue                       # 캡처 경계 겹침 등 완전 동일 → 1건만
        else:
            dup_conflicts.append({
                "reason": "동일 회원 · 점수 불일치 (정책상 불가)",
                "raw": r,
                "candidates": [prev["score"], r["score"]],
                "detail": (
                    f"'{r['name']}' 가 서로 다른 점수 두 건으로 잡혔습니다 "
                    f"({prev['score']:,} / {r['score']:,}). 길드 컨텐츠는 본캐 1개만 "
                    f"참여 가능하므로 정상 상황이 아닙니다. 판독 오류이거나 본캐 환산이 "
                    f"잘못됐을 수 있으니 해당 행을 재확인해 주세요."
                ),
            })
    ambiguous.extend(dup_conflicts)

    final = sorted(dedup.values(), key=lambda r: -r["score"])
    return {
        "resolved": final,
        "ambiguous": ambiguous,
        "stats": {
            "raw_count": len(raw_rows),
            "resolved_count": len(final),
            "ambiguous_count": len(ambiguous),
            "converted_count": sum(1 for r in final if r["note"]),
            "zero_score_count": sum(1 for r in final if r["score"] == 0),
            "duplicates_merged": len(resolved) - len(dedup),
        },
    }
