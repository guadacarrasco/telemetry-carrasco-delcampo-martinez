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


class LapsRepository:
    def __init__(self) -> None:
        self.table = _dynamodb().Table(os.getenv("LAPS_TABLE", "f1_laps"))

    def save(self, lap: dict) -> None:
        item = json.loads(json.dumps(lap, default=str), parse_float=Decimal)
        self.table.put_item(Item=item)

    def list_for_driver(self, session_key: int, driver_number: int) -> list:
        pk = f"{session_key}#{driver_number}"
        resp = self.table.query(
            KeyConditionExpression=Key("session_driver").eq(pk)
        )
        return resp.get("Items", [])
