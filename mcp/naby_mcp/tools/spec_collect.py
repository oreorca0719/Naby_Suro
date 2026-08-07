"""주간 스펙 스냅샷 수집 — 수로 점수 적재와 같은 시점에 1회 실행.

각 회원의 가장 강한 프리셋(보스 세팅)을 찾아 환산 점수를 계산하고
DynamoDB 의 해당 주차 레코드에 함께 저장한다.

주 1회면 충분하다. 스펙은 자주 바뀌지 않으며(실측 5주간 미변경 회원 ±0.7%),
수로 점수도 주간 단위로 정산되기 때문이다.
"""
from __future__ import annotations

import json
import time
import urllib.parse
import urllib.request

from ..config import NEXON_BASE, require_nexon_key
from . import db
from .roster import week_to_date
from .spec import character_spec

# NEXON API 호출 간격 (레이트 리밋 여유)
_DELAY = 0.1


def _get(path: str, params: dict) -> dict:
    key = require_nexon_key()
    url = f"{NEXON_BASE}{path}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={"x-nxopen-api-key": key})
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode("utf-8"))


def fetch_one(name: str, date: str) -> dict | None:
    """캐릭터 1명의 스펙 스냅샷. 조회 실패 시 None."""
    try:
        ocid = _get("/id", {"character_name": name}).get("ocid")
        if not ocid:
            return None
        basic = _get("/character/basic", {"ocid": ocid, "date": date})
        stat = _get("/character/stat", {"ocid": ocid, "date": date})
        equip = _get("/character/item-equipment", {"ocid": ocid, "date": date})
        # 세트효과는 최강 프리셋 아이템으로 직접 계산하므로 set-effect API 불필요
        return character_spec(basic, stat, equip)
    except Exception:
        return None


def collect_week(week: str, names: list[str] | None = None,
                 save: bool = True) -> dict:
    """해당 주차 회원들의 스펙을 수집해 DynamoDB 에 저장한다.

    Args:
        week:  주차 키 YYYYMMDD (정산 종료일=수요일)
        names: 대상 닉네임. 생략하면 해당 주차 전체 회원.
        save:  False 면 계산만 하고 저장하지 않는다(리허설용).

    Returns:
        {"week","date","total","collected","failed","failed_names","rows"}
    """
    date = week_to_date(week)
    rows = db.get_week_rows(week)
    if names:
        target = [r for r in rows if r["name"] in set(names)]
    else:
        target = rows

    collected: list[dict] = []
    failed: list[str] = []

    for r in target:
        spec = fetch_one(r["name"], date)
        time.sleep(_DELAY)
        if not spec or not spec.get("score"):
            failed.append(r["name"])
            continue
        collected.append({
            "name": r["name"],
            "rank": r["rank"],
            "spec_score": spec["score"],
            "spec_set_score": spec.get("set_score", 0),
            "spec_items": spec["item_count"],
            "spec_preset": spec["best_preset"],
            "spec_level": spec["level"],
            "spec_main_stat": spec["main_stat"],
        })
        if save:
            db.table().update_item(
                Key={"week": week, "rank": int(r["rank"])},
                UpdateExpression=(
                    "SET spec_score = :s, spec_set_score = :ss, spec_items = :i, "
                    "spec_preset = :p, spec_level = :l, spec_main_stat = :m"
                ),
                ExpressionAttributeValues={
                    ":s": int(spec["score"]),
                    ":ss": int(spec.get("set_score", 0)),
                    ":i": int(spec["item_count"]),
                    ":p": int(spec["best_preset"]),
                    ":l": int(spec["level"]),
                    ":m": spec["main_stat"],
                },
            )

    return {
        "week": week,
        "date": date,
        "total": len(target),
        "collected": len(collected),
        "failed": len(failed),
        "failed_names": failed,
        "rows": collected,
    }
