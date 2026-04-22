from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from collections import Counter
import boto3
import os

app = FastAPI()

# ── DynamoDB 설정 ──────────────────────────────────────────────────
TABLE_NAME = os.environ.get("DYNAMODB_TABLE", "maple_guild")
AWS_REGION  = os.environ.get("AWS_REGION", "ap-northeast-2")

dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
table    = dynamodb.Table(TABLE_NAME)


def get_latest_week() -> str:
    """메타 아이템에서 최신 주차 조회"""
    resp = table.get_item(Key={"week": "METADATA", "rank": 0})
    item = resp.get("Item")
    if not item:
        raise HTTPException(status_code=404, detail="데이터가 없습니다.")
    return item["latest_week"]


def get_members(week: str) -> list[dict]:
    """해당 주차 길드원 전체 조회"""
    from boto3.dynamodb.conditions import Key
    resp = table.query(
        KeyConditionExpression=Key("week").eq(week)
        & Key("rank").gt(0)
    )
    items = resp.get("Items", [])
    # rank 기준 정렬, Decimal → int 변환
    return sorted(
        [{"rank": int(i["rank"]), "name": i["name"], "job": i["job"], "score": int(i["score"])} for i in items],
        key=lambda x: x["rank"]
    )


@app.get("/api/data")
def get_data():
    week    = get_latest_week()
    members = get_members(week)

    scores  = [m["score"] for m in members if m["score"] > 0]
    job_cnt = Counter(m["job"] for m in members)

    stats = {
        "week":        week,
        "total":       len(members),
        "active":      len(scores),
        "total_score": sum(scores),
        "avg_score":   int(sum(scores) / len(scores)) if scores else 0,
        "max_score":   max(scores) if scores else 0,
    }

    return {
        "stats":            stats,
        "top30":            members[:30],
        "all_members":      members,
        "job_distribution": [{"job": k, "count": v} for k, v in job_cnt.most_common()],
    }


@app.get("/api/health")
def health():
    return {"status": "ok"}


app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
def root():
    return FileResponse("static/index.html")
