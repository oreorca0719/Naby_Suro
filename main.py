from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from collections import Counter, defaultdict
from datetime import datetime, timedelta
import statistics
import boto3
import os

app = FastAPI()

# ── DynamoDB 설정 ──────────────────────────────────────────────────
TABLE_NAME = os.environ.get("DYNAMODB_TABLE", "maple_guild")
AWS_REGION  = os.environ.get("AWS_REGION", "ap-northeast-1")

dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
table    = dynamodb.Table(TABLE_NAME)


def get_latest_week() -> str:
    """메타 아이템에서 최신 주차 조회"""
    resp = table.get_item(Key={"week": "METADATA", "rank": 0})
    item = resp.get("Item")
    if not item:
        raise HTTPException(status_code=404, detail="데이터가 없습니다.")
    return item["latest_week"]


def get_week_display(week: str) -> str:
    """주차 YYYYMMDD를 m/d~m/d 형식으로 변환 (목요일~다음주 수요일)"""
    try:
        wed = datetime.strptime(week, "%Y%m%d")
        thu = wed - timedelta(days=6)
        return f"{thu.month}/{thu.day}~{wed.month}/{wed.day}"
    except:
        return week


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
        "week":           week,
        "week_display":   get_week_display(week),
        "total":          len(members),
        "active":         len(scores),
        "total_score":    sum(scores),
        "avg_score":      int(sum(scores) / len(scores)) if scores else 0,
        "max_score":      max(scores) if scores else 0,
    }

    return {
        "stats":            stats,
        "top30":            members[:30],
        "all_members":      members,
        "job_distribution": [{"job": k, "count": v} for k, v in job_cnt.most_common()],
    }


@app.get("/api/week/{week}")
def get_week(week: str):
    members = get_members(week)
    if not members:
        raise HTTPException(status_code=404, detail="해당 주차 데이터가 없습니다.")
    return {"week": week, "week_display": get_week_display(week), "all_members": members}


@app.get("/api/history")
def get_history():
    from boto3.dynamodb.conditions import Attr
    resp = table.scan(FilterExpression=Attr("rank").gt(0))
    items = resp.get("Items", [])
    while "LastEvaluatedKey" in resp:
        resp = table.scan(
            FilterExpression=Attr("rank").gt(0),
            ExclusiveStartKey=resp["LastEvaluatedKey"]
        )
        items.extend(resp.get("Items", []))

    weeks = defaultdict(list)
    for item in items:
        week = item["week"]
        if week == "METADATA":
            continue
        weeks[week].append(int(item["score"]))

    def quantile(arr, p):
        if not arr: return 0
        s = sorted(arr)
        idx = p * (len(s) - 1)
        lo, hi = int(idx), min(int(idx) + 1, len(s) - 1)
        return int(s[lo] + (s[hi] - s[lo]) * (idx - lo))

    result = []
    for week in sorted(weeks.keys()):
        scores = weeks[week]
        active = [s for s in scores if s > 0]
        mean = int(sum(active) / len(active)) if active else 0
        stddev = int((sum((s - mean) ** 2 for s in active) / len(active)) ** 0.5) if active else 0
        result.append({
            "week": week,
            "week_display": get_week_display(week),
            "total_score": sum(active),
            "active": len(active),
            "mean": mean,
            "stddev": stddev,
            "median_score": quantile(active, 0.5),
            "q1": quantile(active, 0.25),
            "q3": quantile(active, 0.75),
        })
    return result


@app.get("/api/health")
def health():
    return {"status": "ok"}


app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
def root():
    return FileResponse("static/index.html")
