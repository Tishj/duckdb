
import duckdb
import pytest
from conftest import NumpyPandas, ArrowPandas

class TestExecute(object):
    def test_simple_execute(self):
        # we can use duckdb.query to run both DDL statements and select statements
        duckdb.query('create view v1 as select 42 i')
        rel = duckdb.query('select * from v1')
        assert rel.fetchall()[0][0] == 42;

        # also multiple statements
        duckdb.query('create view v2 as select i*2 j from v1; create view v3 as select j * 2 from v2;')
        rel = duckdb.query('select * from v3')
        assert rel.fetchall()[0][0] == 168;

        # we can run multiple select statements - we get only the last result
        res = duckdb.query('select 42; select 84;').fetchall()
        assert res == [(84,)]
