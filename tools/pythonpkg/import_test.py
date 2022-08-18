import duckdb

x = 5
try:
	res = duckdb.default_connection.register("df1", x)
except:
	pass

def test_actual_df():
	import pandas as pd
	x = pd.DataFrame([5, 4, 3])
	res = duckdb.default_connection.register("df2", x)
	print(res)

test_actual_df()
