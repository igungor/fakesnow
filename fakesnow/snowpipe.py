"""Snowpipe's REST API, which loads staged files through a pipe and reports what it loaded.

See https://docs.snowflake.com/en/user-guide/data-load-snowpipe-rest-apis

Files load when a client submits them, before insertFiles returns, rather than in the background as in Snowflake.
Each file loads on its own, so a file that fails doesn't stop the others, as with Snowpipe's default ON_ERROR of
SKIP_FILE. A file the pipe's table has loaded before is skipped, and reported only when it first loaded.
"""

from __future__ import annotations

import datetime
import uuid
from typing import Any

import snowflake.connector.errors
from sqlglot import exp

from fakesnow.dialect import parse_one
from fakesnow.instance import FakeSnow
from fakesnow.transforms.pipe import not_found_error

HISTORY_COLUMNS = (
    "path",
    "stage_location",
    "file_size",
    "time_received",
    "last_insert_time",
    "rows_inserted",
    "rows_parsed",
    "errors_seen",
    "error_limit",
    "first_error",
    "first_error_line_num",
    "first_error_character_pos",
    "first_error_column_name",
    "status",
)


def _parts(pipe: str) -> tuple[str, str, str]:
    """The database, schema and name of a fully qualified pipe name, eg: mydb.myschema.mypipe."""
    table = exp.to_table(pipe, dialect="snowflake")
    if not (table.catalog and table.db):
        raise not_found_error(pipe)
    catalog, schema, name = (
        part.name if isinstance(part, exp.Identifier) and part.quoted else part.name.upper() for part in table.parts
    )
    return catalog, schema, name


def insert_files(fs: FakeSnow, pipe: str, files: list[dict[str, Any]]) -> None:
    """Load the files through the pipe's COPY INTO, and record how each loaded."""
    catalog, schema, name = _parts(pipe)
    time_received = _now()
    copy = parse_one(_definition(fs, catalog, schema, name))
    assert isinstance(copy, exp.Copy)

    # the COPY INTO's names resolve in the pipe's schema
    with fs.connect(catalog, schema) as conn, conn.cursor() as cur:
        for file in files:
            entry = {
                "path": file["path"],
                "stage_location": None,
                "file_size": file.get("size"),
                "time_received": time_received,
                "first_error": None,
                "first_error_line_num": None,
                "first_error_character_pos": None,
                "first_error_column_name": None,
            }
            try:
                cur.execute(_copy_file(copy, file["path"]).sql(dialect="snowflake"))
                result = cur.fetchone()
            except snowflake.connector.errors.ProgrammingError as e:
                entry |= {"rows_inserted": 0, "rows_parsed": 0, "errors_seen": 1, "error_limit": 1}
                entry |= {"first_error": e.msg, "status": "LOAD_FAILED"}
            else:
                assert isinstance(result, tuple)
                _file, status, rows_parsed, rows_loaded, error_limit, errors_seen, first_error, line, char, column = (
                    result
                )
                if status == "LOAD_SKIPPED":
                    # the pipe's table loaded the file before
                    continue
                entry |= {"rows_inserted": rows_loaded, "rows_parsed": rows_parsed, "errors_seen": errors_seen}
                entry |= {"error_limit": error_limit, "first_error": first_error, "first_error_line_num": line}
                entry |= {"first_error_character_pos": char, "first_error_column_name": column, "status": status}

            entry["last_insert_time"] = _now()
            fs.duck_conn.cursor().execute(
                f"""INSERT INTO _fs_global._fs_information_schema._fs_pipe_load_history
                (pipe_catalog, pipe_schema, pipe_name, {", ".join(HISTORY_COLUMNS)})
                VALUES ({", ".join(["?"] * (3 + len(HISTORY_COLUMNS)))})""",
                [catalog, schema, name, *(entry[c] for c in HISTORY_COLUMNS)],
            )


def _definition(fs: FakeSnow, catalog: str, schema: str, name: str) -> str:
    row = (
        fs.duck_conn.cursor()
        .execute(
            "SELECT definition FROM _fs_global._fs_information_schema._fs_pipes "
            "WHERE database_name = ? AND schema_name = ? AND name = ?",
            [catalog, schema, name],
        )
        .fetchone()
    )
    if not row:
        raise not_found_error(f"{catalog}.{schema}.{name}")
    return row[0]


def _copy_file(copy: exp.Copy, path: str) -> exp.Copy:
    """The pipe's COPY INTO for a single file."""
    copy = copy.copy()
    files = exp.CopyParameter(this=exp.var("FILES"), expression=exp.Tuple(expressions=[exp.Literal.string(path)]))
    params = [p for p in copy.args.get("params") or [] if p.name.upper() != "FILES"]
    copy.set("params", [*params, files])
    return copy


def insert_report(fs: FakeSnow, pipe: str, begin_mark: str | None) -> dict[str, Any]:
    """The files loaded through the pipe after the begin mark, see
    https://docs.snowflake.com/en/user-guide/data-load-snowpipe-rest-apis#endpoint-insertreport"""
    catalog, schema, name = _parts(pipe)
    after = int(begin_mark.rpartition("_")[2]) if begin_mark else 0
    files, last = _history(fs, catalog, schema, name, "seq > ?", [after])
    return {
        "pipe": pipe,
        "completeResult": True,
        "nextBeginMark": f"1_{last or after}",
        "files": files,
        "statistics": {"activeFilesCount": 0},
    }


def load_history_scan(fs: FakeSnow, pipe: str, start: str | None, end: str | None) -> dict[str, Any]:
    """The files loaded through the pipe in a time range, see
    https://docs.snowflake.com/en/user-guide/data-load-snowpipe-rest-apis#endpoint-loadhistoryscan"""
    catalog, schema, name = _parts(pipe)
    now = _now()
    start = start or now
    end = end or now
    files, _last = _history(
        fs,
        catalog,
        schema,
        name,
        "last_insert_time >= ?::TIMESTAMPTZ AND last_insert_time < ?::TIMESTAMPTZ",
        [start, end],
    )
    return {
        "pipe": pipe,
        "completeResult": True,
        "startTimeInclusive": start,
        "endTimeExclusive": end,
        "rangeStartTime": files[0]["lastInsertTime"] if files else start,
        "rangeEndTime": files[-1]["lastInsertTime"] if files else end,
        "files": files,
    }


def _history(
    fs: FakeSnow, catalog: str, schema: str, name: str, condition: str, params: list[Any]
) -> tuple[list[dict[str, Any]], int | None]:
    _definition(fs, catalog, schema, name)
    rows = (
        fs.duck_conn.cursor()
        .execute(
            f"""SELECT seq, {", ".join(HISTORY_COLUMNS)} FROM _fs_global._fs_information_schema._fs_pipe_load_history
            WHERE pipe_catalog = ? AND pipe_schema = ? AND pipe_name = ? AND {condition}
            ORDER BY seq""",
            [catalog, schema, name, *params],
        )
        .fetchall()
    )
    files = [_file_entry(dict(zip(HISTORY_COLUMNS, row[1:], strict=True))) for row in rows]
    return files, (rows[-1][0] if rows else None)


def _file_entry(row: dict[str, Any]) -> dict[str, Any]:
    entry = {
        "path": row["path"],
        "stageLocation": row["stage_location"],
        "fileSize": row["file_size"],
        "timeReceived": _iso(row["time_received"]),
        "lastInsertTime": _iso(row["last_insert_time"]),
        "rowsInserted": row["rows_inserted"],
        "rowsParsed": row["rows_parsed"],
        "errorsSeen": row["errors_seen"],
        "errorLimit": row["error_limit"],
        "complete": True,
        "status": row["status"],
    }
    if row["first_error"] is not None:
        entry |= {
            "firstError": row["first_error"],
            "firstErrorLineNum": row["first_error_line_num"],
            "firstErrorCharacterPos": row["first_error_character_pos"],
            "firstErrorColumnName": row["first_error_column_name"],
        }
    return entry


def request_id() -> str:
    return str(uuid.uuid4())


def _now() -> str:
    return _iso(datetime.datetime.now(datetime.UTC))


def _iso(ts: datetime.datetime | str) -> str:
    """A UTC timestamp in the format snowpipe returns, eg: 2017-06-21T04:47:41.453Z"""
    if isinstance(ts, str):
        return ts
    return ts.astimezone(datetime.UTC).strftime("%Y-%m-%dT%H:%M:%S.") + f"{ts.microsecond // 1000:03d}Z"
