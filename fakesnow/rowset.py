from __future__ import annotations

import json
import math
from decimal import Decimal
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc

from fakesnow.rowtype import ColumnInfo

# Results in snowflake's json format, the rowset clients receive when they ask for json results
# rather than arrow, eg: with JDBC_QUERY_RESULT_FORMAT=JSON. Every value is a string or null.


def to_json_rowset(table: pa.Table, rowtype: list[ColumnInfo]) -> list[list[str | None]]:
    columns = [_to_json_column(col, c) for col, c in zip(table.columns, rowtype, strict=True)]
    return [list(row) for row in zip(*columns, strict=True)]


def _to_json_column(col: pa.ChunkedArray, info: ColumnInfo) -> list[str | None]:
    sf_type = info["type"]

    if pa.types.is_timestamp(col.type):
        nanos = col.cast(pa.timestamp("ns", tz=col.type.tz)).cast(pa.int64()).to_pylist()
        # timestamp_tz carries its offset in minutes plus 1440, and fakesnow's are always UTC
        suffix = " 1440" if sf_type == "timestamp_tz" else ""
        return [None if n is None else _decimal_seconds(n) + suffix for n in nanos]
    elif pa.types.is_time(col.type):
        nanos = pc.multiply(col.cast(pa.time64("us")).cast(pa.int64()), 1000).to_pylist()
        return [None if n is None else _decimal_seconds(n) for n in nanos]
    elif pa.types.is_date(col.type):
        return [None if d is None else str(d) for d in col.cast(pa.date32()).cast(pa.int32()).to_pylist()]

    return [None if v is None else _to_json_value(v, sf_type, info) for v in col.to_pylist()]


def _to_json_value(v: Any, sf_type: str, info: ColumnInfo) -> str:  # noqa: ANN401
    if sf_type == "fixed":
        scale = info["scale"] or 0
        return f"{Decimal(v):.{scale}f}" if scale else str(int(v))
    elif sf_type == "real":
        return _real(v)
    elif sf_type == "boolean":
        return "1" if v else "0"
    elif sf_type == "binary":
        return bytes(v).hex().upper()
    elif sf_type in ("variant", "object", "array"):
        return _variant(v)
    return str(v)


def _decimal_seconds(nanos: int) -> str:
    sign = "-" if nanos < 0 else ""
    seconds, fraction = divmod(abs(nanos), 1_000_000_000)
    return f"{sign}{seconds}.{fraction:09d}"


def _real(v: float) -> str:
    """Format a float as snowflake does, eg: in TO_VARCHAR.

    Snowflake keeps at most 9 digits after the decimal point, and between 10 and 15 significant digits.
    """
    if math.isnan(v):
        return "NaN"
    elif math.isinf(v):
        return "inf" if v > 0 else "-inf"
    elif v == 0:
        return "0"

    integer_digits = 0 if abs(v) < 1 else math.floor(math.log10(abs(v))) + 1
    precision = min(15, max(10, integer_digits + 9))
    return f"{v:.{precision}g}"


class _Number:
    """A json number as written, so it's output without being converted to a float and back."""

    def __init__(self, text: str):
        self.text = text


def _variant(text: str) -> str:
    """Format json the way snowflake outputs semi-structured values, ie: indented by 2 spaces."""
    value = json.loads(text, parse_int=_Number, parse_float=_Number, parse_constant=_Number)
    return _dump(value, 0)


def _dump(value: Any, level: int) -> str:  # noqa: ANN401
    if isinstance(value, _Number):
        return value.text
    elif isinstance(value, dict):
        if not value:
            return "{}"
        indent = "  " * (level + 1)
        items = [f"{indent}{json.dumps(k, ensure_ascii=False)}: {_dump(v, level + 1)}" for k, v in value.items()]
        return "{\n" + ",\n".join(items) + "\n" + "  " * level + "}"
    elif isinstance(value, list):
        if not value:
            return "[]"
        indent = "  " * (level + 1)
        items = [f"{indent}{_dump(v, level + 1)}" for v in value]
        return "[\n" + ",\n".join(items) + "\n" + "  " * level + "]"
    return json.dumps(value, ensure_ascii=False)
