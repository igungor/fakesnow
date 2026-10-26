"""DDL (Data Definition Language) transforms for fakesnow.

This module contains transformations for DDL statements like ALTER TABLE, CREATE TABLE, etc.

Future DDL transforms can be added here:
- CREATE TABLE enhancements
- DROP TABLE transformations
- ALTER TABLE modifications (beyond ADD COLUMN)
- INDEX operations
- VIEW operations
- etc.
"""

from __future__ import annotations

import secrets
from decimal import Decimal

import snowflake.connector.errors
import sqlglot
from duckdb import DuckDBPyConnection
from sqlglot import Expr, exp

from fakesnow.transforms.transforms import SUCCESS_NOP


def alter_table_add_multiple_columns(
    expression: Expr,
) -> list[Expr]:
    """Transform ALTER TABLE ADD COLUMN with multiple columns into separate statements.

    Snowflake supports: ALTER TABLE IF EXISTS tab1 ADD COLUMN IF NOT EXISTS col1 INT, col2 VARCHAR(50), col3 BOOLEAN;
    DuckDB requires separate statements for each column.

    Args:
        expression: The expression to potentially transform

    Returns:
        List of expressions - multiple ALTER statements if transformation applied,
        otherwise single original expression
    """
    if not isinstance(expression, exp.Alter):
        return [expression]

    actions = expression.args.get("actions")
    if not (
        actions
        and isinstance(actions, list)
        and all(isinstance(action, exp.ColumnDef) for action in actions)
        and len(actions) > 1
    ):
        return [expression]

    # Create separate ALTER statements for each column
    alter_statements = []

    for action in actions:
        new_alter = exp.Alter(
            this=expression.this,
            kind=expression.args.get("kind"),
            exists=expression.args.get("exists"),
            actions=[action],
        )
        alter_statements.append(new_alter)

    return alter_statements


def _is_alter_table_cluster_by(expression: Expr) -> bool:
    if not isinstance(expression, exp.Alter):
        return False
    actions = expression.actions
    if not actions or len(actions) != 1:
        return False
    return isinstance(actions[0], (exp.Cluster, exp.ClusterProperty))


def _is_alter_table_drop_clustering_key(expression: Expr) -> bool:
    if not isinstance(expression, exp.Alter):
        return False
    actions = expression.actions
    if not (actions and len(actions) == 1 and isinstance(actions[0], exp.Command)):
        return False
    action = actions[0]
    return (
        isinstance(action.this, str)
        and action.this.upper() == "DROP"
        and isinstance(action.expression, str)
        and "CLUSTERING" in action.expression.upper()
    )


def _is_alter_table_recluster(expression: Expr) -> bool:
    if not (isinstance(expression, exp.Command) and isinstance(expression.this, str)):
        return False
    if expression.this.upper() != "ALTER":
        return False
    text = expression.expression
    return isinstance(text, str) and ("RESUME RECLUSTER" in text.upper() or "SUSPEND RECLUSTER" in text.upper())


def alter_table_strip_cluster_by(expression: Expr) -> Expr:
    """Turn Snowflake clustering operations into a no-op.

    DuckDB doesn't support clustering, so the following statements are ignored:
    - ALTER TABLE ... CLUSTER BY (col1, col2)
    - ALTER TABLE ... DROP CLUSTERING KEY
    - ALTER TABLE ... RESUME RECLUSTER
    - ALTER TABLE ... SUSPEND RECLUSTER
    """
    if _is_alter_table_cluster_by(expression):
        return SUCCESS_NOP
    if _is_alter_table_drop_clustering_key(expression):
        return SUCCESS_NOP
    if _is_alter_table_recluster(expression):
        return SUCCESS_NOP
    return expression


def create_table_autoincrement(
    expression: Expr,
) -> list[Expr]:
    """Split CREATE TABLE with AUTOINCREMENT into CREATE SEQUENCE + CREATE TABLE with DEFAULT NEXTVAL.

    Example transform:
        CREATE TABLE test_table (id NUMERIC NOT NULL AUTOINCREMENT, name VARCHAR)
        ->
        CREATE SEQUENCE test_table_id_seq START 1;
        CREATE TABLE test_table (id NUMERIC NOT NULL DEFAULT NEXTVAL('test_table_id_seq'), name VARCHAR)
    """

    if not (
        isinstance(expression, exp.Create)
        and expression.kind == "TABLE"
        and (schema := expression.this)
        and (table := schema.this)
        and isinstance(schema, exp.Schema)
        and isinstance(table, exp.Table)
        # Find AUTOINCREMENT/IDENTITY columns
        and (
            auto_cols := [
                cd
                for cd in (schema.expressions or [])
                if isinstance(cd, exp.ColumnDef)
                and (cd.find(exp.AutoIncrementColumnConstraint) or cd.find(exp.GeneratedAsIdentityColumnConstraint))
            ]
        )
    ):
        return [expression]

    if len(auto_cols) > 1:
        raise NotImplementedError("Multiple AUTOINCREMENT columns")

    auto = auto_cols[0]
    col_name = auto.this.name
    table_name = table.name
    # When recreating the same table with a sequence, we need to give the sequence a unique name to avoid
    # Dependency Error: Cannot drop entry "_FS_SEQ_..." because there are entries that depend on it.
    random_suffix = secrets.token_hex(4)
    seq_name = f"_fs_seq_{table_name}_{col_name}_{random_suffix}"

    # Build CREATE SEQUENCE, using START/INCREMENT if provided
    start_val = "1"
    increment_val = "1"

    identity = auto.find(exp.GeneratedAsIdentityColumnConstraint)
    if identity:
        s = identity.args.get("start")
        i = identity.args.get("increment")
        if isinstance(s, exp.Literal):
            start_val = s.this
        if isinstance(i, exp.Literal):
            increment_val = i.this

    seq_stmt = sqlglot.parse_one(
        f"CREATE SEQUENCE {seq_name} START WITH {start_val} INCREMENT BY {increment_val}",
        read="duckdb",
    )

    # Build modified CREATE TABLE with DEFAULT NEXTVAL('<seq_name>') and without AUTOINCREMENT
    new_create: exp.Create = expression.copy()
    new_schema: exp.Schema = new_create.this

    # Find the corresponding column in the copied schema
    target_col = next(
        cd for cd in (new_schema.expressions or []) if isinstance(cd, exp.ColumnDef) and cd.this.name == col_name
    )

    existing_constraints: list[Expr] = target_col.args.get("constraints", []) or []

    # Replace the AUTOINCREMENT/IDENTITY constraint in-place with DEFAULT NEXTVAL('<seq_name>')
    for c in existing_constraints:
        if isinstance(c, exp.ColumnConstraint) and isinstance(
            c.args.get("kind"),
            (
                exp.AutoIncrementColumnConstraint,
                exp.GeneratedAsIdentityColumnConstraint,
            ),
        ):
            c.set(
                "kind",
                exp.DefaultColumnConstraint(
                    this=exp.Anonymous(
                        this="NEXTVAL",
                        expressions=[exp.Literal(this=seq_name, is_string=True)],
                    )
                ),
            )
            break

    target_col.set("constraints", existing_constraints)

    return [seq_stmt, new_create]


def default_decimal_literal(expression: Expr) -> Expr:
    """Write a decimal literal column default in its shortest form, as snowflake shows it, eg: 12.34000000 as 12.34.

    The value is the same, only how SHOW COLUMNS and information_schema show it changes.
    """
    if (
        isinstance(expression, exp.DefaultColumnConstraint)
        and isinstance(
            literal := expression.this.this if isinstance(expression.this, exp.Neg) else expression.this, exp.Literal
        )
        and not literal.is_string
        and "." in literal.this
        and "e" not in literal.this.lower()
    ):
        literal.set("this", format(Decimal(literal.this).normalize(), "f"))
    return expression


_T = exp.DataType.Type

# snowflake's data type, as it names it, for each of sqlglot's types
_SF_TYPES = {
    **dict.fromkeys([_T.DECIMAL, _T.INT, _T.BIGINT, _T.SMALLINT, _T.TINYINT], "NUMBER"),
    **dict.fromkeys([_T.DOUBLE, _T.FLOAT], "FLOAT"),
    **dict.fromkeys([_T.VARCHAR, _T.TEXT, _T.CHAR, _T.NCHAR, _T.NVARCHAR], "TEXT"),
    **dict.fromkeys([_T.BINARY, _T.VARBINARY], "BINARY"),
    **dict.fromkeys([_T.TIMESTAMP, _T.TIMESTAMPNTZ, _T.DATETIME], "TIMESTAMP_NTZ"),
    _T.TIMESTAMPLTZ: "TIMESTAMP_LTZ",
    _T.TIMESTAMPTZ: "TIMESTAMP_TZ",
    _T.BOOLEAN: "BOOLEAN",
    _T.DATE: "DATE",
    _T.TIME: "TIME",
    _T.VARIANT: "VARIANT",
    _T.OBJECT: "OBJECT",
    _T.ARRAY: "ARRAY",
}


def alter_column_type(
    expression: Expr, duck_conn: DuckDBPyConnection, current_database: str | None, current_schema: str | None
) -> Expr:
    """Reject the column type changes snowflake doesn't support: to another data type, or to another number scale.

    See https://docs.snowflake.com/en/sql-reference/sql/alter-table-column
    """
    if not (
        isinstance(expression, exp.Alter)
        and expression.kind == "TABLE"
        and isinstance(table := expression.this, exp.Table)
        and (catalog := table.catalog or current_database)
        and (schema := table.db or current_schema)
    ):
        return expression

    for action in expression.args.get("actions") or []:
        if not (isinstance(action, exp.AlterColumn) and isinstance(dtype := action.args.get("dtype"), exp.DataType)):
            continue
        if not (new_type := _SF_TYPES.get(dtype.this)):
            continue

        duck_conn.execute(
            "select column_name, data_type, character_maximum_length, numeric_precision, numeric_scale "
            "from _fs_global._fs_information_schema._fs_columns "
            "where table_catalog = ? and table_schema = ? and table_name = ? and column_name = ?",
            [catalog, schema, table.name, action.this.name],
        )
        if not (row := duck_conn.fetchone()):
            continue
        column, old_type, old_length, old_precision, old_scale = row

        params = [int(p.this.this) for p in dtype.expressions if isinstance(p.this, exp.Literal)]
        new_precision, new_scale = (params + [38, 0][len(params) :])[:2] if new_type == "NUMBER" else (None, None)
        new_length = (params or [None])[0] if new_type == "TEXT" else None

        old = _render(old_type, old_length, old_precision, old_scale)
        new = _render(new_type, new_length, new_precision, new_scale)
        if old_type != new_type:
            raise snowflake.connector.errors.ProgrammingError(
                msg=f"SQL compilation error: cannot change column {column} from type {old} to {new}", errno=2108
            )
        if new_type == "NUMBER" and new_scale != old_scale:
            raise snowflake.connector.errors.ProgrammingError(
                msg=f"SQL compilation error: cannot change column {column} from type {old} to {new} "
                "because changing the scale of a number is not supported.",
                errno=40052,
            )

    return expression


def _render(sf_type: str, length: int | None, precision: int | None, scale: int | None) -> str:
    """The type as snowflake writes it in error messages, eg: VARCHAR(16777216)."""
    if sf_type == "NUMBER":
        return f"NUMBER({precision if precision is not None else 38},{scale or 0})"
    elif sf_type == "TEXT":
        return f"VARCHAR({length or 16777216})"
    elif sf_type == "BINARY":
        return "BINARY(8388608)"
    elif sf_type.startswith("TIMESTAMP") or sf_type == "TIME":
        return f"{sf_type}(9)"
    return sf_type
