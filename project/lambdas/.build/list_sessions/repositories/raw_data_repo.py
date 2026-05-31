import json
import os

import boto3


def _s3():
    kwargs = {"region_name": os.getenv("AWS_REGION", "us-east-1")}
    endpoint = os.getenv("AWS_ENDPOINT_URL")
    if endpoint:
        kwargs["endpoint_url"] = endpoint
    return boto3.client("s3", **kwargs)


class RawDataRepository:
    def __init__(self) -> None:
        self.client = _s3()
        self.bucket = os.getenv("RAW_BUCKET", "f1-raw-data")

    def put_json(self, key: str, payload) -> None:
        self.client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=json.dumps(payload, default=str).encode("utf-8"),
            ContentType="application/json",
        )
