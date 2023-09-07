import duckdb
import tempfile
import os
import pytest
import pandas as pd
import datetime


class TestConnectionSettings(object):
    def test_register_precedence(self):
        con = duckdb.connect()
        df = pd.DataFrame({'A': [42]})
        df2 = pd.DataFrame({'B': [42]})
        con.register('df', df2)

        res = con.sql('select * from df')
        # 'register' takes precedence over a local variable with the same name
        assert res.columns[0] == 'B'

    def test_native_behavior_with_register(self):
        con = duckdb.connect()
        df2 = pd.DataFrame({'B': [21]})
        con.register('df', df2)
        con.execute('create table df as select * from df tbl(C)')
        res = con.sql('select * from df')
        # 'register' also takes precedence over a table named 'df' in the database
        # (not what I was expecting tbh)
        assert res.columns[0] == 'B'

    def test_native_behavior_with_local(self):
        con = duckdb.connect()
        df = pd.DataFrame({'A': [42]})
        con.execute('create table df as select * from df tbl(C)')
        res = con.sql('select * from df')
        # 'register' also takes precedence over a table named 'df' in the database
        # (not what I was expecting tbh)
        assert res.columns[0] == 'C'

    def test_disabled_local(self):
        con = duckdb.connect()
        df = pd.DataFrame({'A': [42]})
        con.options.scan_variables = False
        with pytest.raises(duckdb.CatalogException):
            # Because we disabled scanning variables, no replacement will be done for 'df'
            con.sql('select * from df')

        con2 = duckdb.connect()
        # Make sure these options are separate per connection
        assert con2.options.scan_variables == True
