import os
from typing import Dict, List

import boto3
from boto3.dynamodb.conditions import Key

from common.constants import (
    GSI1_NAME,
    GSI1_PK_DISOWNED,
    ENV_DST_BUCKET,
    ENV_TABLE_NAME,
    ENV_THRESHOLD_SECONDS,
)
from common.time_utils import cutoff_iso

s3_client = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")

DST_BUCKET_NAME = os.environ[ENV_DST_BUCKET]
TABLE_NAME = os.environ[ENV_TABLE_NAME]
DISOWNED_THRESHOLD_SECONDS = int(os.environ[ENV_THRESHOLD_SECONDS])

table = dynamodb.Table(TABLE_NAME)


def lambda_handler(event, context):
    print("Cleaner triggered.")
    expired_items = get_expired_disowned_items()

    deleted_count = 0
    for item in expired_items:
        delete_disowned_copy(item)
        deleted_count += 1

    return {
        "statusCode": 200,
        "deleted_count": deleted_count,
    }


def get_expired_disowned_items() -> List[Dict]:
    cutoff = cutoff_iso(DISOWNED_THRESHOLD_SECONDS)
    print(f"Cleaner cutoff timestamp: {cutoff}")

    response = table.query(
        IndexName=GSI1_NAME,
        KeyConditionExpression=(
            Key("gsi1_pk").eq(GSI1_PK_DISOWNED) &
            Key("gsi1_sk").lte(cutoff)
        ),
    )

    items = response.get("Items", [])
    print(f"Found {len(items)} expired disowned items")
    return items


def delete_disowned_copy(item: Dict) -> None:
    copy_key = item["copy_key"]
    source_key = item["PK"]
    sort_key = item["SK"]

    delete_destination_object(copy_key)
    delete_mapping_record(source_key, sort_key)


def delete_destination_object(copy_key: str) -> None:
    s3_client.delete_object(
        Bucket=DST_BUCKET_NAME,
        Key=copy_key,
    )
    print(f"Deleted disowned destination object: {copy_key}")


def delete_mapping_record(source_key: str, sort_key: str) -> None:
    table.delete_item(
        Key={
            "PK": source_key,
            "SK": sort_key,
        }
    )
    print(f"Deleted mapping record PK={source_key}, SK={sort_key}")