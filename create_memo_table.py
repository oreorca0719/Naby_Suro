"""
최초 1회 실행: 간부 메모 DynamoDB 테이블 생성

수로 통계 테이블(maple_guild)과 분리한 이유:
  main.py 의 통계 집계가 테이블 전체를 scan 하는데, 그중 이름으로만 필터하는
  스캔이 있어 메모 항목이 통계에 섞여 들어갈 수 있다. 물리적으로 분리한다.
"""

import boto3

AWS_REGION = "ap-northeast-1"
TABLE_NAME = "maple_guild_memo"


def create_table():
    dynamodb = boto3.client("dynamodb", region_name=AWS_REGION)
    try:
        dynamodb.create_table(
            TableName=TABLE_NAME,
            KeySchema=[
                {"AttributeName": "pk", "KeyType": "HASH"},          # 항상 "MEMO"
                {"AttributeName": "created_at", "KeyType": "RANGE"},  # epoch ms = 메모 id
            ],
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "created_at", "AttributeType": "N"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        print(f"테이블 '{TABLE_NAME}' 생성 요청 완료 (리전: {AWS_REGION})")
        dynamodb.get_waiter("table_exists").wait(TableName=TABLE_NAME)
        print("활성화 완료")
    except dynamodb.exceptions.ResourceInUseException:
        print(f"테이블 '{TABLE_NAME}' 이미 존재함")


if __name__ == "__main__":
    create_table()
