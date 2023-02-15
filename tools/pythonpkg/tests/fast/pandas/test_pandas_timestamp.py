import duckdb
import pandas as pd
from pytest import mark

class TestPandasTimestamp:
	@mark.parametrize('timezone', ['UTC', 'CET', 'Asia/Kathmandu'])
	def test_pandas_timestamp_tz(self, timezone):
		con = duckdb.connect()
		con.execute("SET TimeZone = '"+timezone+"'")
		df = pd.DataFrame({"timestamp": [pd.Timestamp("2022-01-01 10:15", tz=timezone)]})
		duck_df = con.from_df(df).df()
		print (df['timestamp'].dtype)
		print(duck_df['timestamp'].dtype)
		print (df)
		print(duck_df)
		assert df.equals(duck_df)
