import duckdb

con = duckdb.connect()
con.execute("create table tbl as select 'hello' i")
con.execute("select i::int from tbl").fetchall()
