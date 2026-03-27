import os
import urllib.parse
from typing import Dict, List, Optional

import boto3
from boto3.dynamodb.conditions import Key

from common.constants import (
    STATUS_ACTIVE,
    STATUS_DISOWNED,
    GSI1_PK_DISOWNED,
    ENV_SRC_BUCKET,
    ENV_DST_BUCKET,
    ENV_TABLE_NAME,
    ENV_MAX_COPIES,
)
from common.time_utils import utc_now, iso_utc, compact_timestamp

s3_client = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")

SRC_BUCKET_NAME = os.environ[ENV_SRC_BUCKET]
DST_BUCKET_NAME = os.environ[ENV_DST_BUCKET]
TABLE_NAME = os.environ[ENV_TABLE_NAME]
MAX_COPIES = int(os.environ[ENV_MAX_COPIES])

table = dynamodb.Table(TABLE_NAME)


def lambda_handler(event, context):
    print("Received event:", event)

    result = process_event(event)

    return {
        "statusCode": 200,
        "result": result,
    }


def process_event(event: Dict):
    source_key = get_source_key(event)
    event_type = get_event_type(event)

    if not source_key or not event_type:
        return {"action": "ignored", "reason": "unsupported_event_shape"}

    print(f"Processing event_type={event_type}, source_key={source_key}")

    if event_type == "Object Created":
        handle_put(source_key)
        return {"source_key": source_key, "action": "replicated"}

    if event_type == "Object Deleted":
        handle_delete(source_key)
        return {"source_key": source_key, "action": "marked_disowned"}

    return {"source_key": source_key, "action": "ignored", "event_type": event_type}


def get_event_type(event: Dict) -> Optional[str]:
    return event.get("detail-type")


def get_source_key(event: Dict) -> Optional[str]:
    detail = event.get("detail", {})
    obj = detail.get("object", {})
    raw_key = obj.get("key")
    if not raw_key:
        return None
    return urllib.parse.unquote_plus(raw_key)


def handle_put(source_key: str) -> None:
    created_dt = utc_now()
    created_iso = iso_utc(created_dt)
    copy_key = build_copy_key(source_key, created_dt)

    create_copy_in_destination(source_key, copy_key)
    put_copy_record(source_key, copy_key, created_iso)
    trim_old_copies_if_needed(source_key)


def handle_delete(source_key: str) -> None:
    mark_all_copies_disowned(source_key)


def build_copy_key(source_key: str, created_dt) -> str:
    safe_ts = compact_timestamp(created_dt)
    return f"{source_key}__{safe_ts}"


def create_copy_in_destination(source_key: str, copy_key: str) -> None:
    s3_client.copy_object(
        Bucket=DST_BUCKET_NAME,
        Key=copy_key,
        CopySource={
            "Bucket": SRC_BUCKET_NAME,
            "Key": source_key,
        },
    )
    print(f"Created copy: {copy_key}")


def put_copy_record(source_key: str, copy_key: str, created_iso: str) -> None:
    item = {
        "PK": source_key,
        "SK": f"COPY#{created_iso}",
        "source_key": source_key,
        "copy_key": copy_key,
        "created_at": created_iso,
        "status": STATUS_ACTIVE,
    }

    table.put_item(Item=item)
    print(f"Inserted mapping record for {copy_key}")


def query_copies_for_source(source_key: str) -> List[Dict]:
    items: List[Dict] = []
    response = table.query(
        KeyConditionExpression=Key("PK").eq(source_key),
        ScanIndexForward=True,
    )
    items.extend(response.get("Items", []))

    while "LastEvaluatedKey" in response:
        response = table.query(
            KeyConditionExpression=Key("PK").eq(source_key),
            ScanIndexForward=True,
            ExclusiveStartKey=response["LastEvaluatedKey"],
        )
        items.extend(response.get("Items", []))

    print(f"Found {len(items)} copies for source_key={source_key}")
    return items


def trim_old_copies_if_needed(source_key: str) -> None:
    items = query_copies_for_source(source_key)
    active_items = [item for item in items if item.get("status") == STATUS_ACTIVE]

    if len(active_items) <= MAX_COPIES:
        print("No trimming needed.")
        return

    overflow_count = len(active_items) - MAX_COPIES
    items_to_delete = active_items[:overflow_count]

    for item in items_to_delete:
        delete_copy_item(item)


def delete_copy_item(item: Dict) -> None:
    copy_key = item["copy_key"]
    pk = item["PK"]
    sk = item["SK"]

    s3_client.delete_object(
        Bucket=DST_BUCKET_NAME,
        Key=copy_key,
    )
    print(f"Deleted old copy from destination bucket: {copy_key}")

    table.delete_item(
        Key={
            "PK": pk,
            "SK": sk,
        }
    )
    print(f"Deleted mapping record PK={pk}, SK={sk}")


def mark_all_copies_disowned(source_key: str) -> None:
    items = query_copies_for_source(source_key)
    if not items:
        print(f"No copies found to disown for source_key={source_key}")
        return

    disowned_iso = iso_utc(utc_now())

    for item in items:
        pk = item["PK"]
        sk = item["SK"]

        table.update_item(
            Key={
                "PK": pk,
                "SK": sk,
            },
            UpdateExpression=(
                "SET #status = :status, "
                "disowned_at = :disowned_at, "
                "gsi1_pk = :gsi1_pk, "
                "gsi1_sk = :gsi1_sk"
            ),
            ExpressionAttributeNames={
                "#status": "status",
            },
            ExpressionAttributeValues={
                ":status": STATUS_DISOWNED,
                ":disowned_at": disowned_iso,
                ":gsi1_pk": GSI1_PK_DISOWNED,
                ":gsi1_sk": disowned_iso,
            },
        )

    print(f"Marked {len(items)} copies as disowned for source_key={source_key}")