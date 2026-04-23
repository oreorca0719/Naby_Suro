"""
DynamoDB 주차 데이터 수정
20260409 -> 20260416으로 변경 (목요일 ~ 다음주 수요일 기준)
"""

import boto3
from boto3.dynamodb.conditions import Key

TABLE_NAME = "maple_guild"
AWS_REGION = "ap-northeast-1"

dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
table = dynamodb.Table(TABLE_NAME)

print("[UPDATE] 20260409 -> 20260416 변경 중...")
resp = table.query(
    KeyConditionExpression=Key("week").eq("20260409")
)
items = resp.get("Items", [])
print(f"   변경할 항목: {len(items)}개")

with table.batch_writer() as batch:
    for item in items:
        batch.delete_item(Key={"week": item["week"], "rank": item["rank"]})
    
    for item in items:
        new_item = item.copy()
        new_item["week"] = "20260416"
        batch.put_item(Item=new_item)

table.put_item(Item={
    "week": "METADATA",
    "rank": 0,
    "latest_week": "20260416",
})

print("[OK] 20260416으로 변경 완료")
print("\n[RESULT] 최종 저장된 주차:")
resp = table.scan(ProjectionExpression="week")
weeks = set(item["week"] for item in resp["Items"])
print(f"   {sorted(weeks)}")
