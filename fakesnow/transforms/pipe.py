from __future__ import annotations

import datetime

import snowflake.connector.errors
import sqlglot
from duckdb import DuckDBPyConnection
from sqlglot import Expr, exp

from fakesnow import copy_into
from fakesnow.transforms import stage

# Pipes, see https://docs.snowflake.com/en/user-guide/data-load-snowpipe-intro. Their COPY INTO runs when a client
# asks to load files through the Snowpipe REST API, see fakesnow/snowpipe.py.

PIPE_COLUMNS = (
    "created_on",
    "name",
    "database_name",
    "schema_name",
    "definition",
    "owner",
    "notification_channel",
    "comment",
    "integration",
    "pattern",
    "error_integration",
    "owner_role_type",
    "invalid_reason",
    "kind",
    "budget",
)


def not_found_error(fqname: str) -> snowflake.connector.errors.ProgrammingError:
    return snowflake.connector.errors.ProgrammingError(
        msg=f"SQL compilation error:\nPipe '{fqname}' does not exist or not authorized.",
        errno=2003,
        sqlstate="02000",
    )


def _parts(table: exp.Table, current_database: str | None, current_schema: str | None) -> tuple[str, str, str]:
    catalog = table.catalog or current_database
    schema = table.db or current_schema
    assert catalog and schema, "check_db_and_schema requires a current database and schema for unqualified names"
    return catalog, schema, table.name


def _exists(duck_conn: DuckDBPyConnection, catalog: str, schema: str, name: str) -> bool:
    duck_conn.execute(
        "SELECT 1 FROM _fs_global._fs_information_schema._fs_pipes "
        "WHERE database_name = ? AND schema_name = ? AND name = ?",
        [catalog, schema, name],
    )
    return duck_conn.fetchone() is not None


def create_pipe(
    expression: Expr,
    duck_conn: DuckDBPyConnection,
    current_database: str | None,
    current_schema: str | None,
    table_stages: stage.TableStages,
) -> Expr:
    """Transform CREATE PIPE to an INSERT into the fake pipes table, after checking its COPY INTO compiles.

    See https://docs.snowflake.com/en/sql-reference/sql/create-pipe
    """
    if not (
        isinstance(expression, exp.Create)
        and expression.kind == "PIPE"
        and isinstance(table := expression.this, exp.Table)
        and isinstance(copy := expression.expression, exp.Copy)
    ):
        return expression

    catalog, schema, name = _parts(table, current_database, current_schema)
    _check_copy(copy, duck_conn, catalog, schema, table_stages)

    comment = ""
    for prop in properties.expressions if (properties := expression.args.get("properties")) else []:
        if isinstance(prop, exp.SchemaCommentProperty) and isinstance(prop.this, exp.Literal):
            comment = prop.this.this

    replace = expression.args.get("replace")
    row = exp.Tuple(
        expressions=[
            exp.Literal.string(datetime.datetime.now(datetime.UTC).isoformat()),
            exp.Literal.string(name),
            exp.Literal.string(catalog),
            exp.Literal.string(schema),
            exp.Literal.string(expression.args["pipe_definition"]),
            exp.Literal.string("SYSADMIN"),
            exp.null(),
            exp.Literal.string(comment),
            exp.null(),
            exp.null(),
            exp.null(),
            exp.Literal.string("ROLE"),
            exp.null(),
            exp.Literal.string("STAGE"),
            exp.null(),
        ]
    )
    guard = (
        ""
        if replace
        else f"""
        WHERE NOT EXISTS (
            SELECT 1 FROM _fs_global._fs_information_schema._fs_pipes
            WHERE database_name = '{catalog}' AND schema_name = '{schema}' AND name = '{name}'
        )"""
    )
    insert_sql = f"""
        INSERT {"OR REPLACE" if replace else ""} INTO _fs_global._fs_information_schema._fs_pipes
        SELECT * FROM (VALUES {row.sql(dialect="duckdb")}) {guard}
    """
    transformed = sqlglot.parse_one(insert_sql, read="duckdb")
    transformed.args["create_pipe_name"] = name
    transformed.args["create_pipe_if_not_exists"] = expression.args.get("exists")
    return transformed


def _check_copy(
    copy: exp.Copy, duck_conn: DuckDBPyConnection, catalog: str, schema: str, table_stages: stage.TableStages
) -> None:
    """Fail as snowflake does when the COPY INTO's table or stage doesn't exist."""
    target = copy.this.this if isinstance(copy.this, exp.Schema) else copy.this
    assert isinstance(target, exp.Table)
    target_catalog, target_schema, target_name = _parts(target, catalog, schema)
    duck_conn.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_catalog = ? AND table_schema = ? AND table_name = ?",
        [target_catalog, target_schema, target_name],
    )
    if not duck_conn.fetchone():
        raise snowflake.connector.errors.ProgrammingError(
            msg=f"SQL compilation error:\nTable '{target_name}' does not exist or not authorized.",
            errno=2003,
            sqlstate="42S02",
        )

    if (source := copy_into.from_source(copy)).startswith("@"):
        copy_into.stage_url_from_var(source[1:], duck_conn, catalog, schema, table_stages, target)


def describe_pipe(
    expression: Expr, duck_conn: DuckDBPyConnection, current_database: str | None, current_schema: str | None
) -> Expr:
    """Transform DESCRIBE PIPE to a select from the fake pipes table.

    See https://docs.snowflake.com/en/sql-reference/sql/desc-pipe
    """
    if not (isinstance(expression, exp.Describe) and expression.args.get("kind") == "PIPE"):
        return expression

    table = expression.this
    assert isinstance(table, exp.Table)
    catalog, schema, name = _parts(table, current_database, current_schema)
    if not _exists(duck_conn, catalog, schema, name):
        raise not_found_error(f"{catalog}.{schema}.{name}")

    return sqlglot.parse_one(
        f"""
        SELECT {", ".join(PIPE_COLUMNS)} FROM _fs_global._fs_information_schema._fs_pipes
        WHERE database_name = '{catalog}' AND schema_name = '{schema}' AND name = '{name}'
        """,
        read="duckdb",
    )


def show_pipes(expression: Expr, current_database: str | None, current_schema: str | None) -> Expr:
    """Transform SHOW PIPES to a select from the fake pipes table.

    See https://docs.snowflake.com/en/sql-reference/sql/show-pipes
    """
    if not (isinstance(expression, exp.Show) and expression.name.upper() == "PIPES"):
        return expression

    scope_kind = expression.args.get("scope_kind")
    table = expression.find(exp.Table)

    if scope_kind == "DATABASE":
        catalog, schema = (table and table.name) or current_database, None
    elif scope_kind == "SCHEMA" and table:
        catalog, schema = table.db or current_database, table.name
    elif scope_kind == "ACCOUNT":
        catalog, schema = None, None
    else:
        catalog, schema = current_database, current_schema

    where = ["1=1"]
    if catalog:
        where.append(f"database_name = '{catalog}'")
    if schema:
        where.append(f"schema_name = '{schema}'")
    if (like := expression.args.get("like")) and isinstance(like, Expr):
        where.append(f"name ilike {like.sql()}")

    return sqlglot.parse_one(
        f"""
        SELECT {", ".join(PIPE_COLUMNS)} FROM _fs_global._fs_information_schema._fs_pipes
        WHERE {" AND ".join(where)}
        ORDER BY database_name, schema_name, name
        """,
        read="duckdb",
    )


def drop_pipe(
    expression: Expr, duck_conn: DuckDBPyConnection, current_database: str | None, current_schema: str | None
) -> Expr:
    """Transform DROP PIPE to a delete from the fake pipes table.

    See https://docs.snowflake.com/en/sql-reference/sql/drop-pipe
    """
    if not (isinstance(expression, exp.Drop) and expression.args.get("kind") == "PIPE"):
        return expression

    (table,) = expression.args["tables"]
    assert isinstance(table, exp.Table)
    catalog, schema, name = _parts(table, current_database, current_schema)
    if not _exists(duck_conn, catalog, schema, name) and not expression.args.get("exists"):
        raise not_found_error(f"{catalog}.{schema}.{name}")

    transformed = sqlglot.parse_one(
        f"""
        DELETE FROM _fs_global._fs_information_schema._fs_pipes
        WHERE database_name = '{catalog}' AND schema_name = '{schema}' AND name = '{name}'
        """,
        read="duckdb",
    )
    transformed.args["drop_pipe_name"] = name
    return transformed
