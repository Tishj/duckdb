import duckdb
import pandas as pd

class TestNullableBoolean(object):
    def test_register_df_containing_nullable_boolean(self, duckdb_cursor):
		df = pandas.DataFrame({"foo": [True, None, False]}, dtype="boolean")
        conn = duckdb.connect(database=":memory:", read_only=False)
        conn.register(df)
        # assert conn.execute('select count(*) from integers').fetchone()[0] == 5