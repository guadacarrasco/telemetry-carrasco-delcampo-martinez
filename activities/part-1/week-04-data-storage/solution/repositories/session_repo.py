import json
import os
from decimal import Decimal

import boto3


class SessionRepository:
    def __init__(self) -> None:
        kwargs = {"region_name": os.getenv("AWS_REGION", "us-east-1")}
        endpoint = os.getenv("AWS_ENDPOINT_URL")
        if endpoint:
            kwargs["endpoint_url"] = endpoint
        self.dynamodb = boto3.resource("dynamodb", **kwargs)
        self.table = self.dynamodb.Table(os.getenv("SESSIONS_TABLE", "f1_sessions"))

    def save(self, session: dict) -> None:
        item = json.loads(json.dumps(session, default=str), parse_float=Decimal)
        self.table.put_item(Item=item)

    def get(self, session_key: int) -> dict:
        resp = self.table.get_item(Key={"session_key": session_key})
        if "Item" not in resp:
            raise LookupError(f"No session with session_key={session_key}")
        return resp["Item"]

    def list_all(self) -> list:
        resp = self.table.scan()
        return resp.get("Items", [])
