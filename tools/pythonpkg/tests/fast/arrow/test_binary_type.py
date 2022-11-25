import duckdb
import pytest

def create_binary_table(type):
    pa = pytest.importorskip("pyarrow")
    schema = pa.schema([("data", type)])
    inputs = [pa.array([b"foo", b"bar", b"baz"], type=type)]
    return pa.Table.from_arrays(inputs, schema=schema)

class TestArrowBinary(object):
    def test_binary_types(self):
        pa = pytest.importorskip("pyarrow")

        # Fixed Size Binary
        arrow_table = create_binary_table(pa.binary(3))
        rel = duckdb.from_arrow(arrow_table)
        res = rel.execute().fetchall()
        assert res == [(b"foo",), (b"bar",), (b"baz",)]

        # Normal Binary
        arrow_table = create_binary_table(pa.binary())
        rel = duckdb.from_arrow(arrow_table)
        res = rel.execute().fetchall()
        assert res == [(b"foo",), (b"bar",), (b"baz",)]

        # Large Binary
        arrow_table = create_binary_table(pa.large_binary())
        rel = duckdb.from_arrow(arrow_table)
        res = rel.execute().fetchall()
        assert res == [(b"foo",), (b"bar",), (b"baz",)]

    