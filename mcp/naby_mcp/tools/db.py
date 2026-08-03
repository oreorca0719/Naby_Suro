"""DynamoDB 접근 — 직전 주차 조회 + 주차 목록.

웹앱(main.py)과 같은 테이블을 공유하지만 코드는 독립적이다.
(MCP 는 리포를 clone 해 단독으로 동작해야 하므로 상위 모듈을 import 하지 않는다)
"""
from __future__ import annotations

import boto3
from boto3.dynamodb.conditions import Key, Attr

from ..config import DYNAMODB_TABLE, AWS_REGION

_table = None


def table():
    global _table
    if _table is None:
        _table = boto3.resource("dynamodb", region_name=AWS_REGION).Table(DYNAMODB_TABLE)
    return _table


def get_week_rows(week: str) -> list[dict]:
    """해당 주차의 확정 회원 행 (자리표시 ghost 제외)."""
    resp = table().query(
        KeyConditionExpression=Key("week").eq(week) & Key("rank").gt(0)
    )
    items = [i for i in resp.get("Items", []) if not i.get("is_ghost")]
    return sorted(
        [
            {
                "rank": int(i["rank"]),
                "name": i.get("name", ""),
                "job": i.get("job", ""),
                "score": int(i.get("score", 0)),
            }
            for i in items
        ],
        key=lambda r: r["rank"],
    )


def list_weeks() -> list[str]:
    """저장된 주차 키 목록(오름차순). METADATA 제외."""
    weeks: set[str] = set()
    resp = table().scan(
        FilterExpression=Attr("rank").gt(0), ProjectionExpression="#w",
        ExpressionAttributeNames={"#w": "week"},
    )
    for i in resp.get("Items", []):
        if i.get("week") and i["week"] != "METADATA":
            weeks.add(i["week"])
    while "LastEvaluatedKey" in resp:
        resp = table().scan(
            FilterExpression=Attr("rank").gt(0), ProjectionExpression="#w",
            ExpressionAttributeNames={"#w": "week"},
            ExclusiveStartKey=resp["LastEvaluatedKey"],
        )
        for i in resp.get("Items", []):
            if i.get("week") and i["week"] != "METADATA":
                weeks.add(i["week"])
    return sorted(weeks)


def get_prev_week(week: str) -> dict:
    """주어진 주차 직전의 확정 데이터. 없으면 rows 빈 리스트."""
    weeks = [w for w in list_weeks() if w < week]
    if not weeks:
        return {"week": None, "rows": []}
    prev = weeks[-1]
    return {"week": prev, "rows": get_week_rows(prev)}


def week_exists(week: str) -> bool:
    resp = table().query(
        KeyConditionExpression=Key("week").eq(week) & Key("rank").gt(0),
        Limit=1,
    )
    return bool(resp.get("Items"))
