from statistics import mean
import duckdb
import time

con = duckdb.connect()

columns = [
	'bool',
	'tinyint',
	'smallint',
	'int',
	'bigint',
	'hugeint',
	'utinyint',
	'usmallint',
	'uint',
	'ubigint',
	'date',
	'time',
	'timestamp',
	'timestamp_s',
	'timestamp_ms',
	'timestamp_ns',
	'time_tz',
	'timestamp_tz',
	'float',
	'double',
	'dec_4_1',
	'dec_9_4',
	'dec_18_6',
	'dec38_10',
	'uuid',
	'interval',
	'varchar',
	'blob',
	'bit',
	'small_enum',
	'medium_enum',
	'large_enum',
	'int_array',
	'double_array',
	'date_array',
	'timestamp_array',
	'timestamptz_array',
	'varchar_array',
	'nested_int_array',
	'struct',
	'struct_of_arrays',
	'array_of_structs',
	'map'
]

excluded = [
	'date',
	'timestamp_s',
	'timestamp_ns',
	'timestamp_ms',
	'timestamp_tz',
	'timestamp',
	'date_array',
	'timestamp_array',
	'timestamptz_array',
]

columns = [col for col in columns if col not in excluded]

projection = ", ".join(columns)

con.execute(f"""
	create table tbl as select
		{projection} from (select * from test_all_types() limit 1 offset 1),
		range(10000)
""")

result_collectors = {
	"native": "fetchall",
	"pandas": "df",
	"arrow": "arrow",
	"numpy": "fetchnumpy"
}

for name, result_collector in result_collectors.items():
	for col in columns:
		times = []
		for i in range(10):
			rel = con.table('tbl')[col]
			start = time.time()
			res = getattr(rel, result_collector)()
			end = time.time()
			diff = end - start
			times.append(diff)
			print(f"{name}_{col}\t{i}\t{diff}")
		average = mean(times)
		print(average)
