from __future__ import annotations

import datetime
from decimal import Decimal

import pyarrow as pa
import pytest

from fakesnow.rowset import to_json_rowset
from fakesnow.rowtype import ColumnInfo


def column(sf_type: str, scale: int | None = None) -> ColumnInfo:
    return {
        "name": "C",
        "database": "",
        "schema": "",
        "table": "",
        "nullable": True,
        "type": sf_type,
        "byteLength": None,
        "length": None,
        "scale": scale,
        "precision": None,
        "collation": None,
    }


def rowset(values: pa.Array, sf_type: str, scale: int | None = None) -> list[str | None]:
    table = pa.table({"C": values})
    return [row[0] for row in to_json_rowset(table, [column(sf_type, scale)])]


# the values snowflake returns for TO_VARCHAR(<value>::FLOAT)
@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (3.140000104904175, "3.140000105"),
        (0.3400000035762787, "0.3400000036"),
        (65.12200164794922, "65.122001648"),
        (1.1100060165426112e-06, "1.110006017e-06"),
        (123456789.123456789, "123456789.123457"),
        (12345678901234567890.0, "1.23456789012346e+19"),
        (0.000123456789123, "0.0001234567891"),
        (9.99999999995, "10"),
        (99999.99999999999, "100000"),
        (1234567890123456.7, "1.23456789012346e+15"),
        (1e21, "1e+21"),
        (1.5e-7, "1.5e-07"),
        (-3.140000104904175, "-3.140000105"),
        (-0.0, "0"),
        (100.0, "100"),
        (float("nan"), "NaN"),
        (float("inf"), "inf"),
        (float("-inf"), "-inf"),
    ],
)
def test_real(value: float, expected: str):
    assert rowset(pa.array([value]), "real") == [expected]


def test_fixed_keeps_scale():
    assert rowset(pa.array([Decimal("42.83")], pa.decimal128(10, 8)), "fixed", scale=8) == ["42.83000000"]


def test_fixed_integer():
    assert rowset(pa.array([12345678901234567], pa.int64()), "fixed", scale=0) == ["12345678901234567"]


def test_boolean():
    assert rowset(pa.array([True, False]), "boolean") == ["1", "0"]


def test_date_is_days_since_epoch():
    assert rowset(pa.array([datetime.date(2020, 5, 12)]), "date") == ["18394"]


def test_time_is_seconds_since_midnight():
    assert rowset(pa.array([datetime.time(15, 2, 33, 123000)]), "time") == ["54153.123000000"]


def test_timestamp_ntz_is_seconds_since_epoch():
    ts = pa.array([datetime.datetime(2020, 5, 12, 15, 2, 33, 500000)], pa.timestamp("us"))
    assert rowset(ts, "timestamp_ntz") == ["1589295753.500000000"]


def test_timestamp_before_epoch():
    ts = pa.array([datetime.datetime(1969, 12, 31, 23, 59, 58, 500000)], pa.timestamp("us"))
    assert rowset(ts, "timestamp_ntz") == ["-1.500000000"]


def test_timestamp_tz_has_utc_offset():
    ts = pa.array([datetime.datetime(2020, 5, 12, 15, 2, 33, tzinfo=datetime.UTC)], pa.timestamp("us", tz="UTC"))
    assert rowset(ts, "timestamp_tz") == ["1589295753.000000000 1440"]


def test_binary_is_hex():
    assert rowset(pa.array([b"blob"]), "binary") == ["626C6F62"]


def test_variant_is_indented():
    variant = pa.array(['{"k":[1,2.50],"o":{},"a":[],"s":"\\u0001ü"}'])
    assert rowset(variant, "variant") == [
        '{\n  "k": [\n    1,\n    2.50\n  ],\n  "o": {},\n  "a": [],\n  "s": "\\u0001ü"\n}'
    ]


def test_variant_scalar():
    assert rowset(pa.array(['"abc"', "1", "null"]), "variant") == ['"abc"', "1", "null"]


def test_null():
    assert rowset(pa.array([None], pa.string()), "text") == [None]
