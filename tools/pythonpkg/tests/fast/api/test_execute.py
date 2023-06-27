
import duckdb
import pytest
from conftest import NumpyPandas, ArrowPandas

class TestExecute(object):
    def test_simple_execute(self, connection):
        res = connection.execute('select 42').fetchall()
        assert res == [(42,)]

    def test_prepared_execute(self, connection):
        res = connection.execute('select $1', [42]).fetchall()
        assert res == [(42,)]

    def test_execute_many(self, connection):
        res = connection.execute('create table tbl (a varchar); select 42').fetchall()
        assert res == [(42,)]
        assert connection.table('tbl').columns == ['a']

    def test_execute_many_prepared(self, connection):
        res = connection.executemany('create table tbl (b bool); select $1', [
            [42]
        ]).fetchall()
        assert res == [(42,)]
        assert connection.table('tbl').columns == ['b']

    def test_execute_insert_many_prepared(self, connection):
        res = connection.execute('create table tbl (a varchar)')
        connection.executemany('create table tbl2 (b bool); insert into tbl select $1', [
            ['a'],
            ['b']
        ])
        assert connection.table('tbl2').columns == ['b']
        res = connection.table('tbl').fetchall()
        assert res == [('a',), ('b',)]

    def test_multi_execute_prepared(self, connection):
        res = connection.execute('create table tbl (a varchar); create table tbl2 (b bool)')
        connection.execute('insert into tbl select $1; insert into tbl2 select $1', [
            [
                ['a'],
                ['b']
            ],
            [
                [True],
                [False]
            ]
        ])
