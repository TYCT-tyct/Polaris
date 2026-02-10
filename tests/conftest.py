from __future__ import annotations

import os
import sys
import asyncio
import socket
import subprocess
import time
from collections.abc import Generator

import psycopg
import pytest
import pytest_asyncio

from polaris.db.pool import Database

if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


def _pick_port() -> int:
    for port in range(55432, 55532):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            if sock.connect_ex(("127.0.0.1", port)) != 0:
                return port
    raise RuntimeError("no free port found for postgres test container")


@pytest.fixture(scope="session")
def postgres_dsn() -> Generator[str, None, None]:
    external = os.getenv("POLARIS_TEST_DATABASE_URL")
    if external:
        yield external
        return

    port = _pick_port()
    cmd = [
        "docker",
        "run",
        "--rm",
        "-d",
        "-e",
        "POSTGRES_PASSWORD=postgres",
        "-e",
        "POSTGRES_DB=polaris",
        "-p",
        f"{port}:5432",
        "postgres:16-alpine",
    ]
    try:
        container_id = subprocess.check_output(cmd, text=True).strip()
    except Exception:
        pytest.skip("docker unavailable and POLARIS_TEST_DATABASE_URL not set")
    dsn = f"postgresql://postgres:postgres@127.0.0.1:{port}/polaris"
    try:
        deadline = time.time() + 45
        while time.time() < deadline:
            try:
                with psycopg.connect(dsn) as conn:
                    with conn.cursor() as cur:
                        cur.execute("select 1")
                        cur.fetchone()
                break
            except Exception:
                time.sleep(1)
        else:
            raise RuntimeError("postgres test container not ready in time")
        yield dsn
    finally:
        subprocess.run(["docker", "rm", "-f", container_id], check=False, capture_output=True, text=True)


@pytest_asyncio.fixture(scope="session")
async def db(postgres_dsn: str) -> Generator[Database, None, None]:
    database = Database(postgres_dsn)
    await database.open()
    await database.apply_migrations()
    yield database
    await database.close()
