from pytest import importorskip

pa = importorskip('pyarrow')

import duckdb


def test_nested(duckdb_cursor):
    res = run(duckdb_cursor, 'select 42::UNION(name VARCHAR, attr UNION(age INT, veteran BOOL)) as res')
    assert pa.types.is_union(res.type)
    assert res.value.value == pa.scalar(42, type=pa.int32())


def test_union_contains_nested_data(duckdb_cursor):
    _ = importorskip("pyarrow", minversion="11")
    res = run(duckdb_cursor, "select ['hello']::UNION(first_name VARCHAR, middle_names VARCHAR[]) as res")
    assert pa.types.is_union(res.type)
    assert res.value == pa.scalar(['hello'], type=pa.list_(pa.string()))


def test_unions_inside_lists_structs_maps(duckdb_cursor):
    res = run(duckdb_cursor, "select [union_value(name := 'Frank')] as res")
    assert pa.types.is_list(res.type)
    assert pa.types.is_union(res.type.value_type)
    assert res[0].value == pa.scalar('Frank', type=pa.string())


def test_unions_with_struct(duckdb_cursor):
    duckdb_cursor.execute(
        """
		CREATE TABLE tbl (a UNION(a STRUCT(a INT, b BOOL)))
	"""
    )
    duckdb_cursor.execute(
        """
		INSERT INTO tbl VALUES ({'a': 42, 'b': true})
	"""
    )

    rel = duckdb_cursor.table('tbl')
    arrow = rel.arrow()

    duckdb_cursor.execute("create table other as select * from arrow")
    rel2 = duckdb_cursor.table('other')
    res = rel2.fetchall()
    assert res == [({'a': 42, 'b': True},)]


def run(conn, query):
    return conn.sql(query).arrow().columns[0][0]
