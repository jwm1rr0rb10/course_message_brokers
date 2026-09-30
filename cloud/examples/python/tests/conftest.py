"""Shared helpers. Each cloud's tests skip themselves if its emulator is not reachable
(or fail, if COURSE_REQUIRE_BROKER=1 as in CI)."""
import os
import socket
import uuid
from urllib.parse import urlparse

import pytest


def reachable(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=2):
            return True
    except OSError:
        return False


def unique(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


AWS_ENDPOINT = os.environ.get("AWS_ENDPOINT_URL", "http://localhost:4566")
PUBSUB_HOST = os.environ.get("PUBSUB_EMULATOR_HOST", "localhost:8085")
SERVICEBUS_CONN = os.environ.get(
    "SERVICEBUS_CONNECTION_STRING",
    "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;"
    "SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;",
)


def require(host_port: str, what: str):
    u = urlparse(host_port if "://" in host_port else f"tcp://{host_port}")
    if not reachable(u.hostname, u.port):
        msg = f"{what} emulator is not running on {u.hostname}:{u.port}"
        if os.environ.get("COURSE_REQUIRE_BROKER") == "1":
            pytest.fail(f"{msg} (COURSE_REQUIRE_BROKER=1)")
        pytest.skip(msg)
