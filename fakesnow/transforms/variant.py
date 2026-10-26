from __future__ import annotations

import duckdb
from duckdb import DuckDBPyConnection
from sqlglot import Expr, exp

# Snowflake converts and compares semi-structured values in ways duckdb's json type doesn't. These transforms
# rewrite those places, using duckdb's own types for the expressions involved.

_STRING_TYPES = {exp.DataType.Type.VARCHAR, exp.DataType.Type.TEXT, exp.DataType.Type.NVARCHAR}

_NUMBER_TYPES = {
    "TINYINT",
    "SMALLINT",
    "INTEGER",
    "BIGINT",
    "HUGEINT",
    "UTINYINT",
    "USMALLINT",
    "UINTEGER",
    "UBIGINT",
    "FLOAT",
    "DOUBLE",
}


class Types:
    """The duckdb types of expressions in a statement, as duckdb resolves them.

    Types are described on a separate connection, because a failed statement aborts the current
    transaction in duckdb, and an expression can't always be described on its own, eg: when it
    references an outer query. That connection only sees committed tables.
    """

    def __init__(self, duck_conn: DuckDBPyConnection, current_database: str | None, current_schema: str | None):
        self.duck_conn = duck_conn
        self.current_database = current_database
        self.current_schema = current_schema
        self._probe_conn: DuckDBPyConnection | None = None
        self._cache: dict[tuple[int, str], str | None] = {}

    def is_variant(self, expression: Expr) -> bool:
        return _is_variant(expression) or (_needs_lookup(expression) and self.of(expression) == "JSON")

    def is_varchar(self, expression: Expr) -> bool:
        return _is_varchar(expression) or (_needs_lookup(expression) and self.of(expression) == "VARCHAR")

    def of(self, expression: Expr) -> str | None:
        """The expression's type, or None when duckdb can't describe it on its own."""
        scope = expression.find_ancestor(exp.Select, exp.Update, exp.Tuple)
        key = (id(scope), expression.sql(dialect="duckdb"))
        if key not in self._cache:
            self._cache[key] = self._describe(expression, scope)
        return self._cache[key]

    def _describe(self, expression: Expr, scope: Expr | None) -> str | None:
        if isinstance(scope, exp.Select):
            probe = _select_probe(scope, expression)
        elif isinstance(scope, exp.Update):
            probe = exp.select(expression.copy()).from_(scope.this.copy())
            if from_ := scope.args.get("from"):
                probe = probe.join(from_.this.copy(), join_type="cross")
        else:
            probe = exp.select(expression.copy())

        if (with_ := expression.root().args.get("with")) and not probe.args.get("with"):
            probe.set("with", with_.copy())

        try:
            conn = self._conn()
            conn.execute(f"DESCRIBE {probe.sql(dialect='duckdb')}")
            return conn.fetchall()[-1][1]
        except duckdb.Error:
            return None

    def _conn(self) -> DuckDBPyConnection:
        if not self._probe_conn:
            (database, schema) = self.duck_conn.execute("select current_database(), current_schema()").fetchall()[0]
            self._probe_conn = self.duck_conn.cursor()
            self._probe_conn.execute(f"SET schema='{database}.{schema}'")
        return self._probe_conn

    def close(self) -> None:
        if self._probe_conn:
            self._probe_conn.close()
            self._probe_conn = None

    def columns(self, table: exp.Table) -> dict[str, str]:
        """The table's column names and their types, in column order."""
        catalog = table.catalog or self.current_database
        schema = table.db or self.current_schema
        if not (catalog and schema):
            return {}
        self.duck_conn.execute(
            "select column_name, data_type from information_schema.columns "
            "where table_catalog = ? and table_schema = ? and table_name = ? order by ordinal_position",
            [catalog, schema, table.name],
        )
        return dict(self.duck_conn.fetchall())


def _select_probe(select: exp.Select, expression: Expr) -> exp.Select:
    """A copy of the select that outputs the expression's value last, so its type can be described."""
    probe = select.copy()
    for arg in ("order", "limit", "offset", "distinct"):
        probe.set(arg, None)

    # an ORDER BY can name an output column, which may be an aggregate
    if (
        isinstance(expression, exp.Column)
        and not expression.table
        and expression.find_ancestor(exp.Order) is not None
        and (projection := next((p for p in select.expressions if p.alias_or_name == expression.name), None))
    ):
        value = projection.unalias().copy()
    else:
        value = expression.copy()

    if (probe.args.get("group") or any(_aggregates(p) for p in probe.expressions)) and not _aggregates(value):
        value = exp.AnyValue(this=value)
    probe.set("expressions", [*probe.expressions, exp.alias_(value, "_fs_probe")])
    return probe


def _aggregates(expression: Expr) -> bool:
    """Whether the expression aggregates rows, as opposed to only using window functions."""
    return any(agg.find_ancestor(exp.Window) is None for agg in expression.find_all(exp.AggFunc))


def _needs_lookup(expression: Expr) -> bool:
    return not isinstance(expression, (exp.Literal, exp.Null, exp.Boolean, exp.Star, exp.Placeholder))


def _is_variant(expression: Expr) -> bool:
    """Whether the expression is known to produce a semi-structured value, without looking up types."""
    if isinstance(expression, exp.Paren):
        return _is_variant(expression.this)
    return isinstance(expression, (exp.JSONExtract, exp.ParseJSON, exp.StripNullValue, exp.GetIgnoreCase)) or (
        isinstance(expression, exp.Anonymous) and expression.name.lower() == "_fs_get_ignore_case"
    )


def _is_varchar(expression: Expr) -> bool:
    """Whether the expression is known to produce a string, without looking up types."""
    if isinstance(expression, exp.Paren):
        return _is_varchar(expression.this)
    elif isinstance(expression, exp.Literal):
        return expression.is_string
    elif isinstance(expression, exp.Cast):
        return expression.to.this in _STRING_TYPES
    return isinstance(
        expression,
        (
            exp.JSONExtractScalar,
            exp.Hex,
            exp.Base64Encode,
            exp.Upper,
            exp.Lower,
            exp.Trim,
            exp.Concat,
            exp.DPipe,
            exp.Substring,
        ),
    )


def _as_varchar(expression: Expr) -> Expr:
    """Convert a semi-structured value to varchar the way snowflake does, ie: a string without its quotes."""
    return exp.JSONExtractScalar(this=expression, expression=exp.Literal(this="$", is_string=True))


# functions whose arguments are strings, so snowflake converts a semi-structured argument to varchar
_VARCHAR_ARGUMENT_FUNCTIONS = (
    exp.ParseJSON,
    exp.Hex,
    exp.Base64Encode,
    exp.Upper,
    exp.Lower,
    exp.Trim,
    exp.Concat,
    exp.DPipe,
    exp.Substring,
)


def variant_as_varchar_argument(expression: Expr, types: Types) -> Expr:
    """Convert a semi-structured argument of a function that takes strings to varchar, as snowflake does.

    Eg: PARSE_JSON(v:json_text) parses the string v:json_text holds, rather than returning it.
    """
    if isinstance(expression, _VARCHAR_ARGUMENT_FUNCTIONS):
        for key in ("this", "expression", "expressions"):
            arg = expression.args.get(key)
            if isinstance(arg, list):
                expression.set(key, [_as_varchar(a) if types.is_variant(a) else a for a in arg])
            elif isinstance(arg, Expr) and types.is_variant(arg):
                expression.set(key, _as_varchar(arg))
    return expression


def assignment_casts(expression: Expr, types: Types) -> Expr:
    """Convert the values an INSERT or UPDATE assigns to columns, as snowflake does implicitly.

    - a semi-structured value assigned to a column of any other type is converted from its varchar
      form, ie: a string without its quotes, and to a number column true and false are 1 and 0.
    - a string assigned to a binary column is decoded as hex, the default BINARY_INPUT_FORMAT.
    """
    if isinstance(expression, exp.Insert):
        target = expression.this
        table = target.this if isinstance(target, exp.Schema) else target
        if not isinstance(table, exp.Table):
            return expression
        column_types = types.columns(table)
        names = (
            [c.name for c in target.expressions]
            if isinstance(target, exp.Schema) and target.expressions
            else list(column_types)
        )

        source = expression.expression
        if isinstance(source, exp.Select):
            rows = [source]
        elif isinstance(source, exp.Values):
            rows = [t for t in source.expressions if isinstance(t, exp.Tuple)]
        else:
            return expression

        for row in rows:
            values = row.expressions
            if len(values) != len(names):
                continue
            row.set(
                "expressions",
                [_assign(v, column_types.get(n), types) for v, n in zip(values, names, strict=True)],
            )

    elif isinstance(expression, exp.Update) and isinstance(table := expression.this, exp.Table):
        column_types = types.columns(table)
        for eq in expression.expressions:
            if isinstance(eq, exp.EQ) and isinstance(eq.this, exp.Column):
                eq.set("expression", _assign(eq.expression, column_types.get(eq.this.name), types))

    return expression


def _assign(value: Expr, column_type: str | None, types: Types) -> Expr:
    if column_type is None or column_type == "JSON":
        return value

    inner = value.unalias()
    is_number = column_type.startswith("DECIMAL") or column_type in _NUMBER_TYPES
    if is_number and types.is_variant(inner):
        converted: Expr = exp.Anonymous(this="_fs_variant_number", expressions=[inner])
    elif column_type != "BLOB" and types.is_variant(inner):
        converted = _as_varchar(inner)
    elif column_type == "BLOB" and types.is_varchar(inner):
        converted = exp.Unhex(this=inner)
    else:
        return value

    if isinstance(value, exp.Alias):
        value.set("this", converted)
        return value
    return converted


def _variant_key(expression: Expr) -> Expr:
    return exp.Anonymous(this="_fs_variant_key", expressions=[expression])


def variant_ordering(expression: Expr, types: Types) -> Expr:
    """Order, partition, aggregate and compare semi-structured values the way snowflake does.

    duckdb compares json as text, so eg: 10 sorts before 9, and 1 and 1.0 are distinct. snowflake compares
    by type, then by value. Applies to ORDER BY, PARTITION BY, MIN, MAX, COUNT(DISTINCT) and comparisons
    of two semi-structured values.
    """
    if isinstance(expression, exp.Ordered) and types.is_variant(expression.this):
        expression.set("this", _variant_key(expression.this))
    elif isinstance(expression, exp.Window) and (partition := expression.args.get("partition_by")):
        expression.set("partition_by", [_variant_key(p) if types.is_variant(p) else p for p in partition])
    elif (
        isinstance(expression, (exp.Min, exp.Max)) and not expression.expressions and types.is_variant(expression.this)
    ):
        klass = exp.ArgMin if isinstance(expression, exp.Min) else exp.ArgMax
        return klass(this=expression.this, expression=_variant_key(expression.this.copy()))
    elif (
        isinstance(expression, exp.Count)
        and isinstance(distinct := expression.this, exp.Distinct)
        and len(distinct.expressions) == 1
        and types.is_variant(distinct.expressions[0])
    ):
        distinct.set("expressions", [_variant_key(distinct.expressions[0])])
    elif (
        isinstance(expression, (exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE))
        # an UPDATE's SET assigns rather than compares
        and not isinstance(expression.parent, exp.Update)
        and types.is_variant(expression.this)
        and types.is_variant(expression.expression)
    ):
        expression.set("this", _variant_key(expression.this))
        expression.set("expression", _variant_key(expression.expression))
    return expression
