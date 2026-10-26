# ruff: noqa: E501

from __future__ import annotations

import re

import pytest
import snowflake.connector.cursor
from snowflake.connector.errors import ProgrammingError

DEFINITION = "copy into t(RECORD_METADATA, RECORD_CONTENT) from (select $1:meta, $1:content from @st t) file_format = (type = 'json')"


@pytest.fixture
def pcur(cur: snowflake.connector.cursor.SnowflakeCursor) -> snowflake.connector.cursor.SnowflakeCursor:
    cur.execute("create table t (record_metadata variant, record_content variant)")
    cur.execute("create stage st")
    return cur


def test_create_pipe(pcur: snowflake.connector.cursor.SnowflakeCursor):
    pcur.execute(f"create pipe p as {DEFINITION}")
    assert pcur.fetchall() == [("Pipe P successfully created.",)]


def test_create_pipe_if_not_exists(pcur: snowflake.connector.cursor.SnowflakeCursor):
    pcur.execute(f"create pipe p as {DEFINITION}")
    pcur.execute("create pipe if not exists p as copy into t from @st")
    assert pcur.fetchall() == [("P already exists, statement succeeded.",)]

    pcur.execute("desc pipe p")
    assert pcur.fetchall()[0][4] == DEFINITION


def test_create_pipe_already_exists(pcur: snowflake.connector.cursor.SnowflakeCursor):
    pcur.execute(f"create pipe p as {DEFINITION}")
    with pytest.raises(ProgrammingError, match=re.escape("Object 'P' already exists.")):
        pcur.execute(f"create pipe p as {DEFINITION}")


def test_create_or_replace_pipe(pcur: snowflake.connector.cursor.SnowflakeCursor):
    pcur.execute(f"create pipe p as {DEFINITION}")
    pcur.execute("create or replace pipe p as copy into t from @st")
    pcur.execute("desc pipe p")
    assert pcur.fetchall()[0][4] == "copy into t from @st"


def test_create_pipe_missing_table(pcur: snowflake.connector.cursor.SnowflakeCursor):
    with pytest.raises(ProgrammingError) as excinfo:
        pcur.execute("create pipe p as copy into missing from @st")
    assert (
        str(excinfo.value)
        == "002003 (42S02): SQL compilation error:\nTable 'MISSING' does not exist or not authorized."
    )


def test_create_pipe_missing_stage(pcur: snowflake.connector.cursor.SnowflakeCursor):
    with pytest.raises(ProgrammingError) as excinfo:
        pcur.execute("create pipe p as copy into t from @missing")
    assert (
        str(excinfo.value)
        == "002003 (02000): SQL compilation error:\nStage 'DB1.SCHEMA1.MISSING' does not exist or not authorized."
    )


def test_create_pipe_identifier(pcur: snowflake.connector.cursor.SnowflakeCursor):
    pcur.execute(f"create pipe if not exists identifier('p') as {DEFINITION}")
    pcur.execute("desc pipe identifier('db1.schema1.p')")
    assert pcur.fetchall()[0][1:4] == ("P", "DB1", "SCHEMA1")


def test_describe_pipe(pcur: snowflake.connector.cursor.SnowflakeCursor):
    pcur.execute(f"create pipe p comment = 'loads t' as {DEFINITION}")
    pcur.execute("describe pipe p")
    assert [d.name for d in pcur.description] == [
        "created_on", "name", "database_name", "schema_name", "definition", "owner", "notification_channel", "comment",
        "integration", "pattern", "error_integration", "owner_role_type", "invalid_reason", "kind", "budget",
    ]  # fmt: skip
    (row,) = pcur.fetchall()
    assert row[1:8] == ("P", "DB1", "SCHEMA1", DEFINITION, "SYSADMIN", None, "loads t")
    assert row[11:14] == ("ROLE", None, "STAGE")


def test_describe_pipe_missing(pcur: snowflake.connector.cursor.SnowflakeCursor):
    with pytest.raises(ProgrammingError) as excinfo:
        pcur.execute("desc pipe missing")
    assert (
        str(excinfo.value)
        == "002003 (02000): SQL compilation error:\nPipe 'DB1.SCHEMA1.MISSING' does not exist or not authorized."
    )


def test_show_pipes(pcur: snowflake.connector.cursor.SnowflakeCursor):
    pcur.execute(f"create pipe p1 as {DEFINITION}")
    pcur.execute(f"create pipe p2 as {DEFINITION}")
    pcur.execute("create schema schema2")
    pcur.execute("create table schema2.t (record_metadata variant, record_content variant)")
    pcur.execute("create pipe schema2.p3 as copy into schema2.t from @schema1.st")

    pcur.execute("show pipes in schema db1.schema1")
    assert [r[1] for r in pcur.fetchall()] == ["P1", "P2"]

    pcur.execute("show pipes like 'p_' in database db1")
    assert [r[1] for r in pcur.fetchall()] == ["P1", "P2", "P3"]

    pcur.execute("show pipes like '%3' in account")
    assert [r[1] for r in pcur.fetchall()] == ["P3"]


def test_drop_pipe(pcur: snowflake.connector.cursor.SnowflakeCursor):
    pcur.execute(f"create pipe p as {DEFINITION}")
    pcur.execute("drop pipe p")
    assert pcur.fetchall() == [("P successfully dropped.",)]

    pcur.execute("show pipes")
    assert pcur.fetchall() == []


def test_drop_pipe_missing(pcur: snowflake.connector.cursor.SnowflakeCursor):
    with pytest.raises(ProgrammingError) as excinfo:
        pcur.execute("drop pipe missing")
    assert (
        str(excinfo.value)
        == "002003 (02000): SQL compilation error:\nPipe 'DB1.SCHEMA1.MISSING' does not exist or not authorized."
    )

    pcur.execute("drop pipe if exists missing")
