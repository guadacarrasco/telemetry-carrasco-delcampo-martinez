import json
import os
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Key


class DriverStatsRepository:
    def __init__(self) -> None:
        kwargs = {"region_name": os.getenv("AWS_REGION", "us-east-1")}
        endpoint = os.getenv("AWS_ENDPOINT_URL")
        if endpoint:
            kwargs["endpoint_url"] = endpoint
        self.dynamodb = boto3.resource("dynamodb", **kwargs)
        self.table = self.dynamodb.Table(os.getenv("DRIVER_STATS_TABLE", "f1_driver_stats"))

    def save(self, driver_row: dict) -> None:
        item = json.loads(json.dumps(driver_row, default=str), parse_float=Decimal)
        self.table.put_item(Item=item)

    def get(self, session_key: int, driver_number: int) -> dict:
        resp = self.table.get_item(
            Key={"session_key": session_key, "driver_number": driver_number}
        )
        if "Item" not in resp:
            raise LookupError(
                f"No driver stats for session_key={session_key} driver_number={driver_number}"
            )
        return resp["Item"]

    def list_for_session(self, session_key: int) -> list:
        resp = self.table.query(
            KeyConditionExpression=Key("session_key").eq(session_key),
        )
        return resp.get("Items", [])
