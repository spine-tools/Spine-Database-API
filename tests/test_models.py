from datetime import datetime, timedelta
import json

from dateutil.relativedelta import relativedelta
import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

from spinedb_api.models import (
    arrow_to_array,
    dict_to_array,
    from_dict,
    from_json,
    get_json_schema,
    has_any_array,
    to_json,
    type_map,
    AllArrays,
    AnyArray,
    Array,
    ArrayIndex,
    DictEncodedArray,
    DictEncodedIndex,
    RunEndArray,
    RunEndIndex,
)
from spinedb_api.value_support import to_union_array

good_data = [
    (ArrayIndex, {"name": "dummy", "values": list(range(5)), "value_type": "int", "type": "array_index"}),
    (Array, {"name": "dummy", "values": [1, 2, None, 4], "value_type": "int", "type": "array"}),
    (ArrayIndex, {"name": "dummy", "values": list("abc"), "value_type": "str", "type": "array_index"}),
    (Array, {"name": "dummy", "values": ["a", None, "b"], "value_type": "str", "type": "array"}),
    (
        DictEncodedArray,
        {
            "name": "dummy",
            "values": ["a", "b"],
            "indices": [0, 0, 1, 1, 1],
            "value_type": "str",
            "type": "dict_encoded_array",
        },
    ),
    (
        RunEndArray,
        {"name": "dummy", "values": ["a", "b"], "run_end": [2, 4], "value_type": "str", "type": "run_end_array"},
    ),
    (
        AnyArray,
        {
            "name": "dummy",
            "values": [1, "a", None, "b", 3.14],
            "value_types": ["int", "str", "null", "str", "float"],
            "type": "any_array",
        },
    ),
    (
        ArrayIndex,
        {
            "name": "dummy",
            "values": [
                "2019-01-01T00:00:00",
                pd.Timestamp("2019-01-01T00:30:00"),
                datetime.fromisoformat("2019-01-01T01:00:00"),
            ],
            "value_type": "date_time",
            "type": "array_index",
        },
    ),
    (
        ArrayIndex,
        {
            "name": "dummy",
            "values": [
                "P3DT2H5M",
                timedelta(days=1, minutes=5),
                relativedelta(months=1),
                pd.DateOffset(months=2, days=2),
                pa.MonthDayNano(range(3)),
            ],
            "value_type": "duration",
            "type": "array_index",
        },
    ),
]


@pytest.mark.parametrize("arr_t,kwargs", good_data)
def test_array_dataclass(arr_t, kwargs):
    arr = arr_t(**kwargs)
    match kwargs:
        case {"type": "any_array"}:
            assert kwargs["value_types"] == list(map(lambda i: "null" if i is None else type_map[type(i)], arr.values))
        case _:
            types = set(map(lambda i: type_map[type(i)], filter(None, arr.values)))
            assert len(types) == 1
            assert kwargs["value_type"] in types


@pytest.mark.parametrize("arr_t,data", good_data)
def test_dict_to_array(arr_t, data):
    arr = dict_to_array(data)
    assert isinstance(arr, arr_t)


bad_data = [
    (
        {
            "name": "dummy",
            "values": ["foo", 1, "P1M", pd.DateOffset(months=2, days=2)],
            "value_type": "duration",
            "type": "array_index",
        },
        ValueError,  # ours
        "unable to parse as duration",
    ),
    (
        {
            "name": "dummy",
            "values": [
                "2019-01-01T00:00:00",
                "foo",
                1,
                "2019-01-01T01:00:00",
            ],
            "value_type": "date_time",
            "type": "array_index",
        },
        ValueError,  # from pydantic
        "Invalid isoformat string",
    ),
    (
        {
            "name": "dummy",
            "values": [1, "a", None, "b", 3.14],
            "value_types": ["int", "str", "str", "float"],
            "type": "any_array",
        },
        ValueError,  # ours
        "mismatching lengths:",
    ),
    (
        {
            "name": "dummy",
            "values": [1, "a", None, "b", 3.14],
            "value_types": ["int", "str", "null", "str", "int"],
            "type": "any_array",
        },
        ValueError,  # from pydantic
        "should be a valid integer, got a number with a fractional part",
    ),
    ({"type": "not_an_array"}, ValueError, "unknown array type"),  # ours
]


@pytest.mark.parametrize("kwargs,exc,msg", bad_data)
def test_validation_errs(kwargs, exc, msg):
    with pytest.raises(exc, match=msg):
        dict_to_array(kwargs)


pa_data = [
    (pa.array(list(range(6))), ArrayIndex),
    (pa.array([1, 2, None, 4, 5, None]), Array),
    (pa.array(list("ab") * 3).dictionary_encode(), DictEncodedIndex),
    (pa.array(["a", "s", "a", "a", None, "s"]).dictionary_encode(), DictEncodedArray),
    (pa.RunEndEncodedArray.from_arrays([2, 6], "foo bar".split()), RunEndIndex),
    (pa.RunEndEncodedArray.from_arrays([2, 5, 6], ["foo", None, "bar"]), RunEndArray),
    (to_union_array(["a", "b", None, 3, 4, 5]), AnyArray),
]


@pytest.mark.parametrize("arr,arr_t", pa_data)
def test_arrow_to_array(arr, arr_t):
    res = arrow_to_array("foo", arr)
    assert isinstance(res, arr_t)


bad_pa_data = [(np.arange(6), ValueError, "column.+is an unsupported array type")]


@pytest.mark.parametrize("arr,exc,msg", bad_pa_data)
def test_arrow_to_array_err(arr, exc, msg):
    with pytest.raises(exc, match=msg):
        arrow_to_array("foo", arr)


@pytest.mark.parametrize("arr_t,data", good_data)
def test_serde(arr_t, data):
    arr = from_dict(data)
    assert isinstance(arr, arr_t)
    json_str = to_json(arr)
    if data.get("value_type") not in ("date_time", "duration", "time_period", None):
        # NOTE: these require special conversion, json.loads won't match & any_array
        res = json.loads(json_str)
        res.pop("metadata")
        assert res == data
    assert from_json(json_str, AllArrays) == arr


def test_serde_err():
    with pytest.raises(ValueError, match="unsupported type"):
        from_dict(set())  # type: ignore


example_data = [
    {
        "name": "col_1",
        "value_type": "str",
        "values": ["foo", "bar", "baz", None, None],
        "type": "array",
    },
    {
        "name": "timestamp",
        "value_type": "date_time",
        "values": ["2025-01-01T12:00:00", "2025-01-02T00:00:00"],
        "type": "array",
    },
    {
        "name": "value",
        "value_type": "float",
        "values": [3.14, 2.718, 4.2, 3.14, 2.718],
        "type": "array",
    },
]
# simple conf
conf1 = [
    {
        "name": "key",
        "value_type": "str",
        "values": ["tolerance", "flag", "path", "option1", "option2"],
        "type": "array",
    },
    {
        "name": "value",
        "value_types": ["float", "bool", "str", "str", "null"],
        "values": [1e-6, True, "/path/to/file.csv", "do_xyz", None],
        "type": "any_array",
    },
]
# alternate more complex conf
conf2 = [
    {
        "name": "col_1",
        "value_type": "str",
        "values": ["foo", "bar", "baz"],
        "type": "array",
    },
    {
        "name": "col_2",
        "value_types": ["float", "str", "null"],
        "values": [1e-6, "blabla", None],
        "type": "any_array",
    },
    {
        "name": "col_3",
        "value_types": ["int", "str", "float"],
        "values": [42, "something", 3.14],
        "type": "any_array",
    },
]
pa_conf = pa.RecordBatch.from_arrays([pa.array(range(3)), to_union_array(["a", None, 3])], names=["k", "v"])


@pytest.mark.parametrize("data, expect", [(example_data, False), (conf1, True), (conf2, True), (pa_conf, True)])
def test_is_config(data: dict | pa.RecordBatch, expect: bool):
    match data:
        case list() | dict():
            tbl = from_dict(data)
            assert expect == has_any_array(tbl)
        case pa.RecordBatch():
            assert expect == has_any_array(data)


def test_json_schema():
    schema = get_json_schema()
    assert schema["type"] == "array"
