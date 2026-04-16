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


class SessionRepository:
    def __init__(self) -> None:
        self.table = _dynamodb().Table(os.getenv("SESSIONS_TABLE", "f1_sessions"))

    def save(self, session: dict) -> None:
        item = json.loads(json.dumps(session, default=str), parse_float=Decimal)
        self.table.put_item(Item=item)

    def get(self, session_key: int) -> dict:
        resp = self.table.get_item(Key={"session_key": session_key})
        if "Item" not in resp:
            raise LookupError(f"Session {session_key} not found")
        return resp["Item"]

    def list_all(self) -> list:
        resp = self.table.scan()
        return resp.get("Items", [])
