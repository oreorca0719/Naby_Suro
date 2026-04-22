"""
최초 1회 실행: DynamoDB 테이블 생성
"""

import boto3

AWS_REGION = "ap-northeast-1"
TABLE_NAME = "maple_guild"

def create_table():
    dynamodb = boto3.client("dynamodb", region_name=AWS_REGION)

    try:
        dynamodb.create_table(
            TableName=TABLE_NAME,
            KeySchema=[
                {"AttributeName": "week", "KeyType": "HASH"},   # PK
                {"AttributeName": "rank", "KeyType": "RANGE"},  # SK
            ],
            AttributeDefinitions=[
                {"AttributeName": "week", "AttributeType": "S"},
                {"AttributeName": "rank", "AttributeType": "N"},
            ],
            BillingMode="PAY_PER_REQUEST",  # 온디맨드 (소규모 트래픽에 최적)
        )
        print(f"✅ 테이블 '{TABLE_NAME}' 생성 완료 (리전: {AWS_REGION})")
    except dynamodb.exceptions.ResourceInUseException:
        print(f"ℹ️  테이블 '{TABLE_NAME}' 이미 존재함")

if __name__ == "__main__":
    create_table()
