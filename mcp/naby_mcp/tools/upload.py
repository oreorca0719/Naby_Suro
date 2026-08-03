"""DynamoDB 적재 — 승인 게이트 + 주차 가드.

실제 적재 로직(_upload_impl)은 운영에서 검증된 upload_to_dynamodb.py 를
그대로 가져온 것이다. 벌금 이월 / 자리표시(ghost) / 재가입 처리가 포함된다.
여기서는 그 위에 안전장치만 덧씌운다:
  - approved 플래그 없이는 절대 쓰지 않는다 (미검증 데이터 유입 차단)
  - 주차 키가 정산 종료일(수요일)인지 확인
  - 이미 존재하는 주차면 덮어쓰기 여부를 명시적으로 확인
"""
from __future__ import annotations

from datetime import datetime

from . import _upload_impl, db


def check_week(week: str) -> dict:
    """적재 전 주차 키 사전 점검 (쓰기 없음)."""
    problems: list[str] = []

    if len(week) != 8 or not week.isdigit():
        problems.append(f"주차 키는 YYYYMMDD 형식이어야 합니다: {week!r}")
        return {"week": week, "ok": False, "problems": problems, "exists": False}

    try:
        d = datetime.strptime(week, "%Y%m%d")
    except ValueError:
        problems.append(f"실재하지 않는 날짜입니다: {week}")
        return {"week": week, "ok": False, "problems": problems, "exists": False}

    if d.weekday() != 2:  # 0=월 ... 2=수
        problems.append(
            f"{week} 는 {'월화수목금토일'[d.weekday()]}요일입니다. "
            f"주차 키는 정산 종료일인 수요일이어야 합니다."
        )

    exists = db.week_exists(week)
    return {
        "week": week,
        "date": d.strftime("%Y-%m-%d"),
        "weekday": "월화수목금토일"[d.weekday()],
        "ok": not problems,
        "problems": problems,
        "exists": exists,
        "note": "이미 해당 주차 데이터가 있습니다. 적재 시 같은 순위 행을 덮어씁니다."
        if exists else "",
    }


def upload_week(
    xlsx_path: str,
    week: str,
    approved: bool = False,
    overwrite: bool = False,
) -> dict:
    """검증·승인이 끝난 xlsx 를 DynamoDB 에 적재한다.

    Args:
        xlsx_path: build_xlsx 가 만든 파일 경로
        week:      주차 키(YYYYMMDD, 정산 종료일=수요일)
        approved:  관리자 승인 여부. False 면 적재하지 않는다.
        overwrite: 이미 존재하는 주차에 덮어쓸지 여부
    """
    if not approved:
        return {
            "uploaded": False,
            "reason": "관리자 승인(approved=True)이 없어 적재하지 않았습니다.",
        }

    check = check_week(week)
    if not check["ok"]:
        return {"uploaded": False, "reason": "주차 키 오류", "detail": check["problems"]}
    if check["exists"] and not overwrite:
        return {
            "uploaded": False,
            "reason": f"{week} 주차 데이터가 이미 존재합니다. "
                      f"덮어쓰려면 overwrite=True 로 다시 호출하세요.",
        }

    _upload_impl.upload(xlsx_path, week)
    return {
        "uploaded": True,
        "week": week,
        "xlsx": xlsx_path,
        "overwritten": check["exists"],
    }
