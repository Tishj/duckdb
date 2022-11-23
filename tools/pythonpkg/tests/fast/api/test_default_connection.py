import duckdb
import tempfile
import os
import pytest

class TestDefaultConnection(object):
    def test_connection_close(self, duckdb_cursor):
        default_conn = duckdb.default_connection
        also_default_conn = duckdb.connect() # does not create a new connection
        not_default_conn = duckdb.connect(':memory:')

        # Create a table on the default connection
        default_conn.execute("create table non_existant_table(i integer)").fetchall()

        ## This will error, because the table already exists
        with pytest.raises(duckdb.CatalogException):
            also_default_conn.execute("create table non_existant_table(i integer)").fetchall()

        # This is fine because we created a new in-memory connection
        not_default_conn.execute("create table non_existant_table(i integer)")
