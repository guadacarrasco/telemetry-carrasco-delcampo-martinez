import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "layer", "python"))

from repositories import SessionRepository
from utils import error, ok


def handler(event, context):
    try:
        sessions = SessionRepository().list_all()
    except Exception as e:
        return error(500, str(e))
    return ok(sessions)
