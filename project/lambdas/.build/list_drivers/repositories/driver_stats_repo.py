import json
import os
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Key


def _dynamodb():
    kwargs = {"region_name": os.getenv("AWS_REGION", "us-east-1")}
    endpoint = os.getenv("AWS_ENDPOINT_URL")
    if endpoint:
        kwargs["endpoint_url"] = endpoint
    return boto3.resource("dynamodb", **kwargs)


class DriverStatsRepository:
    def __init__(self) -> None:
        self.table = _dynamodb().Table(os.getenv("DRIVER_STATS_TABLE", "f1_driver_stats"))

    def save(self, driver: dict) -> None:
        item = json.loads(json.dumps(driver, default=str), parse_float=Decimal)
        self.table.put_item(Item=item)

    def get(self, session_key: int, driver_number: int) -> dict:
        resp = self.table.get_item(
            Key={"session_key": session_key, "driver_number": driver_number}
        )
        if "Item" not in resp:
            raise LookupError(
                f"Driver {driver_number} not found in session {session_key}"
            )
        return resp["Item"]

    def list_for_session(self, session_key: int) -> list:
        resp = self.table.query(
            KeyConditionExpression=Key("session_key").eq(session_key)
        )
        return resp.get("Items", [])
