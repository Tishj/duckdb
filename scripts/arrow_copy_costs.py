import duckdb
import argparse
import time

parser = argparse.ArgumentParser()
parser.add_argument("--verbose", action="store_true", help="Enable verbose mode", default=False)
parser.add_argument("--threads", type=int, help="Number of threads", default=None)
parser.add_argument("--nruns", type=int, help="Number of runs", default=10)
parser.add_argument("--out-file", type=str, help="Output file path", default=None)
parser.add_argument('--disable-zero-copy', action='store_true', help="Disable zero-copy conversion")
args, unknown_args = parser.parse_known_args()


def print_msg(message: str):
    if not args.verbose:
        return
    print(message)


def write_result(benchmark_name, nrun, t):
    bench_result = f"{benchmark_name}\t{nrun}\t{t}"
    if args.out_file is not None:
        if not hasattr(write_result, 'file'):
            write_result.file = open(args.out_file, 'w+')
        write_result.file.write(bench_result)
        write_result.file.write('\n')
    else:
        print_msg(bench_result)


def close_result():
    if not hasattr(write_result, 'file'):
        return
    write_result.file.close()


### --- Zero Copy

types = {
    'NULL',
    'TINYINT',
    'UTINYINT',
    'SMALLINT',
    'USMALLINT',
    'INTEGER',
    'UINTEGER',
    'BIGINT',
    'UBIGINT',
    'HUGEINT',
    'UHUGEINT',
    'FLOAT',
    'DOUBLE',
    'TIMESTAMP',
    'TIMESTAMP_NS',
    'TIMESTAMP_S',
    'TIMESTAMP_MS',
    'DATE',  # <-- date32 [days] in Arrow
    'DECIMAL(38, 10)',
}

# Arrays of this type are kept alive and their data buffers are referenced in the DuckDB Vectors
# If no destructive transformation is applied to them

BATCH_SIZES = [2048, 122880, 122880 * 8, 2000000]

TUPLE_COUNTS = [20480000, 204800, 200000000]

con = duckdb.connect()
con.execute("pragma threads=1")
print(args.disable_zero_copy)
con.execute(f"pragma arrow_disable_zero_copy={args.disable_zero_copy}")
con.query("select current_setting('arrow_disable_zero_copy')").show()

# Measure performance, memory usage

# Create a giant PyArrow Table containing all the rows

# Warm up the analyzer
con.query("explain analyze select 42")

def get_query(type: str):
    if type == 'NULL':
        return "select [NULL, NULL, NULL] as lst"
    type_mapping = {
        'DECIMAL(38, 10)': 'dec38_10',
        'DECIMAL(18, 6)': 'dec_18_6',
        'DECIMAL(9, 4)': 'dec_9_4',
        'DECIMAL(4, 1)': 'dec_4_1',
    }
    if type in type_mapping:
        type = type_mapping[type]
    return f"""select list("{type}") as lst from test_all_types()"""

for type in types:
    for batch_size in BATCH_SIZES:
        for size in TUPLE_COUNTS:
            for nrun in range(10):
                list_query = get_query(type)

                # Create the DuckDB relation we will use to create the arrow table
                rel = con.query(
                    f"""
                    with t as materialized (
                        select
                            lst
                        from (
                            {list_query}
                        ) t(lst)
                    )
                    select CASE
                        WHEN (i % 3 == 0) THEN lst[1]
                        WHEN (i % 3 == 1) THEN lst[2]
                        WHEN (i % 3 == 2) THEN lst[3]
                    END from range({size}) tbl(i), t
                """
                )
                arrow_table = rel.arrow(batch_size=batch_size)

                duration = 0.0
                start = time.time()
                res = con.query("EXPLAIN ANALYZE select * from arrow_table").fetchall()
                end = time.time()
                duration = float(end - start)
                write_result(f"arrow_table_{type}_tuple_count_{size}_batch_size_{batch_size}", nrun, duration)

### --- Partial Copy

types = [
    'VARCHAR',
    'BLOB',
]

# We measure the difference in cost between converting small (inlined) strings, which are fully copied
# And longer strings (over 12 bytes), for which only the first 4 bytes are copied.

### --- Full Copy

types = [
    'BOOLEAN',
    'DATE',  # <-- date64 [milliseconds] in Arrow
    'TIME',
    'TIMESTAMPTZ',
    'INTERVAL',
    'DECIMAL(4, 1)',  # <-- DECIMAL (SMALLINT)
    'DECIMAL(9, 4)',  # <-- DECIMAL (INTEGER)
    'DECIMAL(18, 6)',  # <-- DECIMAL (INTEGER)
]

# For these types we always copy

close_result()
