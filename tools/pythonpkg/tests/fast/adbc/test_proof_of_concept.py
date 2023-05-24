import pytest
import duckdb

class TestADBCConnection(object):
	def test_adbc_connection(self):
		con = duckdb.adbc.connect()
		assert con.__class__ == duckdb.adbc.Connection
		assert con.sql('select 42').fetchall() == [(42,)]

		# Method that doesn't exist on default duckdb
		assert con.new_method() == 'duckdb'
