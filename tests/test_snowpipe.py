from __future__ import annotations

import gzip
import json
import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest
import requests
import snowflake.connector
import snowflake.connector.cursor
from dirty_equals import IsStr

AUTH = {"Authorization": "Bearer jwt", "X-Snowflake-Authorization-Token-Type": "KEYPAIR_JWT"}
DEFINITION = (
    "copy into t(RECORD_METADATA, RECORD_CONTENT) from (select $1:meta, $1:content from @st t) "
    "file_format = (type = 'json')"
)


@pytest.fixture
def pipe(server: dict) -> Iterator[tuple[snowflake.connector.cursor.SnowflakeCursor, str]]:
    # the snowpipe REST API has no session, so it loads into the server's shared databases rather than an
    # isolated session's
    shared = server | {"session_parameters": {"CLIENT_OUT_OF_BAND_TELEMETRY_ENABLED": False}}
    database = f"db_{uuid.uuid4().hex[:8]}"
    with snowflake.connector.connect(**shared) as conn, conn.cursor() as cur:
        cur.execute(f"create database {database}")
        cur.execute(f"create schema {database}.s")
        cur.execute(f"use schema {database}.s")
        cur.execute("create table t (record_metadata variant, record_content variant)")
        cur.execute("create stage st")
        cur.execute(f"create pipe p as {DEFINITION}")
        yield cur, f"http://{server['host']}:{server['port']}/v1/data/pipes/{database}.s.p"


def put(cur: snowflake.connector.cursor.SnowflakeCursor, tmp_path: Path, name: str, content: bytes) -> None:
    path = tmp_path / name
    path.write_bytes(gzip.compress(content))
    cur.execute(f"put 'file://{path}' @st/topic/0 auto_compress=false")


def records(*ids: int) -> bytes:
    return b"\n".join(json.dumps({"meta": {"offset": i}, "content": {"id": i}}).encode() for i in ids)


def insert_files(url: str, *paths: str) -> requests.Response:
    return requests.post(
        f"{url}/insertFiles?requestId=r1&showSkippedFiles=false",
        headers=AUTH,
        json={"files": [{"path": p, "size": 1} for p in paths]},
        timeout=10,
    )


def test_insert_files(pipe: tuple[snowflake.connector.cursor.SnowflakeCursor, str], tmp_path: Path) -> None:
    cur, url = pipe
    put(cur, tmp_path, "a.json.gz", records(1, 2))

    response = insert_files(url, "topic/0/a.json.gz")
    assert response.status_code == 200
    assert response.json() == {"requestId": "r1", "responseCode": "SUCCESS"}

    cur.execute("select record_metadata:offset::int, record_content:id::int from t order by 1")
    assert cur.fetchall() == [(1, 1), (2, 2)]


def test_insert_report(pipe: tuple[snowflake.connector.cursor.SnowflakeCursor, str], tmp_path: Path) -> None:
    cur, url = pipe
    put(cur, tmp_path, "a.json.gz", records(1, 2))
    insert_files(url, "topic/0/a.json.gz")

    report = requests.get(f"{url}/insertReport?requestId=r2", headers=AUTH, timeout=10).json()
    assert report["completeResult"] is True
    assert [(f["path"], f["status"], f["rowsInserted"], f["complete"]) for f in report["files"]] == [
        ("topic/0/a.json.gz", "LOADED", 2, True)
    ]
    assert report["files"][0]["lastInsertTime"] == IsStr(regex=r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z$")

    # nothing loaded after the begin mark
    report = requests.get(f"{url}/insertReport?beginMark={report['nextBeginMark']}", headers=AUTH, timeout=10).json()
    assert report["files"] == []


def test_load_history_scan(pipe: tuple[snowflake.connector.cursor.SnowflakeCursor, str], tmp_path: Path) -> None:
    cur, url = pipe
    put(cur, tmp_path, "a.json.gz", records(1))
    insert_files(url, "topic/0/a.json.gz")

    history = requests.get(
        f"{url}/loadHistoryScan?startTimeInclusive=2000-01-01T00:00:00.000Z&endTimeExclusive=2999-01-01T00:00:00.000Z",
        headers=AUTH,
        timeout=10,
    ).json()
    assert [(f["path"], f["status"]) for f in history["files"]] == [("topic/0/a.json.gz", "LOADED")]

    history = requests.get(
        f"{url}/loadHistoryScan?startTimeInclusive=2000-01-01T00:00:00.000Z&endTimeExclusive=2000-01-02T00:00:00.000Z",
        headers=AUTH,
        timeout=10,
    ).json()
    assert history["files"] == []


def test_insert_files_loads_each_file_once(
    pipe: tuple[snowflake.connector.cursor.SnowflakeCursor, str], tmp_path: Path
) -> None:
    cur, url = pipe
    put(cur, tmp_path, "a.json.gz", records(1))
    insert_files(url, "topic/0/a.json.gz")
    insert_files(url, "topic/0/a.json.gz")

    cur.execute("select count(*) from t")
    assert cur.fetchall() == [(1,)]
    report = requests.get(f"{url}/insertReport", headers=AUTH, timeout=10).json()
    assert len(report["files"]) == 1


def test_insert_files_skips_a_failing_file(
    pipe: tuple[snowflake.connector.cursor.SnowflakeCursor, str], tmp_path: Path
) -> None:
    cur, url = pipe
    put(cur, tmp_path, "bad.json.gz", b"{not json")
    put(cur, tmp_path, "good.json.gz", records(1))

    assert insert_files(url, "topic/0/bad.json.gz", "topic/0/good.json.gz").status_code == 200

    cur.execute("select record_content:id::int from t")
    assert cur.fetchall() == [(1,)]
    report = requests.get(f"{url}/insertReport", headers=AUTH, timeout=10).json()
    assert [(f["path"], f["status"], f["errorsSeen"]) for f in report["files"]] == [
        ("topic/0/bad.json.gz", "LOAD_FAILED", 1),
        ("topic/0/good.json.gz", "LOADED", 0),
    ]
    assert report["files"][0]["firstError"]


def test_dropped_pipe_forgets_its_history(
    pipe: tuple[snowflake.connector.cursor.SnowflakeCursor, str], tmp_path: Path
) -> None:
    cur, url = pipe
    put(cur, tmp_path, "a.json.gz", records(1))
    insert_files(url, "topic/0/a.json.gz")

    cur.execute("drop pipe p")
    cur.execute(f"create pipe p as {DEFINITION}")
    assert requests.get(f"{url}/insertReport", headers=AUTH, timeout=10).json()["files"] == []


def test_missing_pipe(pipe: tuple[snowflake.connector.cursor.SnowflakeCursor, str]) -> None:
    _cur, url = pipe
    response = insert_files(url.replace(".p", ".missing"), "a.json.gz")
    assert response.status_code == 404
    assert response.json()["success"] is False


def test_missing_authorization(pipe: tuple[snowflake.connector.cursor.SnowflakeCursor, str]) -> None:
    _cur, url = pipe
    response = requests.get(f"{url}/insertReport", timeout=10)
    assert response.status_code == 401
