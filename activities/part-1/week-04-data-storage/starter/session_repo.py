"""
Week 4: Data Storage — Session Repository (Starter)

TODO: Implement DynamoDB operations for sessions.
"""
import boto3
import os


class SessionRepository:
    def __init__(self):
        kwargs = {"region_name": os.getenv("AWS_REGION", "us-east-1")}
        endpoint = os.getenv("AWS_ENDPOINT_URL")
        if endpoint:
            kwargs["endpoint_url"] = endpoint
        self.dynamodb = boto3.resource("dynamodb", **kwargs)
        self.table = self.dynamodb.Table(os.getenv("SESSIONS_TABLE", "f1_sessions"))

    def save(self, session: dict) -> None:
        """Save a session to DynamoDB."""
        # TODO: Use put_item to save the session
        pass

    def get(self, session_key: int) -> dict:
        """Get a session by session_key."""
        # TODO: Use get_item to retrieve the session
        # Return the Item from the response
        # Raise an error if not found
        pass

    def list_all(self) -> list:
        """List all sessions."""
        # TODO: Use scan to get all sessions
        # Return the Items from the response
        pass
