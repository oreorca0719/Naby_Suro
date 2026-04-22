"""
매주 실행: CSV → DynamoDB 업로드 스크립트
사용법: python upload_to_dynamodb.py guild_members_latest.csv
"""

import csv
import sys
import boto3
from datetime import datetime
from decimal import Decimal

# ── 설정 ──────────────────────────────────────────────────────────
TABLE_NAME = "maple_guild"
AWS_REGION  = "ap-northeast-2"
# AWS 자격증명은 환경변수 또는 ~/.aws/credentials 에서 자동 로드
# ────────────────────────────────────────────────────────────────

def upload(csv_path: str):
    week = datetime.now().strftime("%Y%m%d")
    dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
    table    = dynamodb.Table(TABLE_NAME)

    members = []
    with open(csv_path, encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            members.append({
                "week":  week,
                "rank":  int(row["순위"]),
                "name":  row["닉네임"].strip(),
                "job":   row["직업"].strip(),
                "score": Decimal(row["지하수로"].replace(",", "")),
            })

    print(f"📦 {len(members)}명 DynamoDB 업로드 시작... (week={week})")

    with table.batch_writer() as batch:
        for m in members:
            batch.put_item(Item=m)

    # 최신 주차 메타 업데이트
    table.put_item(Item={
        "week":        "METADATA",
        "rank":        0,
        "latest_week": week,
    })

    print(f"✅ 완료! {len(members)}명 업로드됨 → week={week}")


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else "guild_members_latest.csv"
    upload(path)
