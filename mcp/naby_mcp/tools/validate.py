"""교차검증 — 신규/이탈 + 숫자 이상 플래그.

중요: 이 단계는 아무것도 자동으로 고치거나 차단하지 않는다.
점수는 외부 정답지가 존재할 수 없으므로(열린 값) 자동 검증이 불가능하다.
따라서 관리자의 '시선을 어디에 둘지' 안내하는 것이 유일한 목적이다.
"""
from __future__ import annotations

from ..config import (
    SCORE_CHANGE_FLAG_RATIO,
    SCORE_MIN_PLAUSIBLE,
    SCORE_MAX_PLAUSIBLE,
)


def cross_validate(
    resolved: list[dict],
    prev_week_rows: list[dict] | None = None,
    prev_week: str | None = None,
) -> dict:
    """확정 데이터를 직전 주차와 대조해 확인 권장 항목을 뽑는다.

    Returns:
        {
          "newcomers": [...], "leavers": [...],
          "flags": [{"name","reason","score","prev_score","change_pct"}, ...],
          "summary": {...}
        }
    """
    prev_rows = prev_week_rows or []
    prev_by_name = {r["name"]: r for r in prev_rows}
    cur_by_name = {r["name"]: r for r in resolved}

    newcomers = sorted(set(cur_by_name) - set(prev_by_name))
    leavers = sorted(set(prev_by_name) - set(cur_by_name))

    flags: list[dict] = []
    for r in resolved:
        score = r["score"]

        # 1) 자릿수 이상 — OCR 오인식 의심 (0점 미참은 정상이므로 제외)
        if score != 0 and not (SCORE_MIN_PLAUSIBLE <= score <= SCORE_MAX_PLAUSIBLE):
            flags.append({
                "name": r["name"],
                "reason": "점수 범위 이상 (자릿수 오인식 의심)",
                "score": score,
                "prev_score": prev_by_name.get(r["name"], {}).get("score"),
                "change_pct": None,
            })
            continue

        # 2) 직전 주차 대비 급변 — 오인식일 수도, 실제 급락/급등일 수도
        prev = prev_by_name.get(r["name"])
        if prev and prev["score"] > 0 and score > 0:
            change = (score - prev["score"]) / prev["score"]
            if abs(change) >= SCORE_CHANGE_FLAG_RATIO:
                flags.append({
                    "name": r["name"],
                    "reason": f"직전 주차 대비 {change*100:+.0f}% 변동",
                    "score": score,
                    "prev_score": prev["score"],
                    "change_pct": round(change * 100, 1),
                })

    return {
        "prev_week": prev_week,
        "newcomers": newcomers,
        "leavers": leavers,
        "flags": flags,
        "summary": {
            "current_count": len(resolved),
            "prev_count": len(prev_rows),
            "newcomer_count": len(newcomers),
            "leaver_count": len(leavers),
            "flag_count": len(flags),
            "total_score": sum(r["score"] for r in resolved),
            "zero_score_count": sum(1 for r in resolved if r["score"] == 0),
        },
    }
