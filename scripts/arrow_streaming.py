import argparse
import time
import pyarrow.parquet as pq
import duckdb
import pyarrow.dataset as ds

parser = argparse.ArgumentParser()
parser.add_argument("--verbose", action="store_true", help="Enable verbose mode", default=False)
parser.add_argument("--nruns", type=int, help="Number of runs", default=10)
parser.add_argument("--out-file", type=str, help="Output file path", default=None)
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

### -- DuckDB ---

## Transforms Query Result from DuckDB to Arrow Table

## Open dataset using year,month folder partition
#nyc = ds.dataset('nyc-taxi/', partitioning=["year", "month"])
## Get database connection
#con = duckdb.connect()


#for run in range(args.nruns):
#    start = time.time()

#    # Run query that selects part of the data
#    query = con.execute("""
#        SELECT
#            total_amount,
#            passenger_count,year
#        FROM nyc
#        where
#            total_amount > 100
#            and year > 2014
#    """)

#    # Create Record Batch Reader from Query Result.
#    # "fetch_record_batch()" also accepts an extra parameter related to the desired produced chunk size.
#    record_batch_reader = query.fetch_record_batch()

#    # Retrieve all batch chunks
#    while True:
#        try:
#            chunk = record_batch_reader.read_next_batch()
#        except StopIteration:
#            break

#    end = time.time()
#    diff = end - start
#    duration = float(end - start)
#    write_result(f"benchmark_result", run, duration)

### --- Pandas ---

import pyarrow as pa
import pandas as pd


for run in range(args.nruns):
    start = time.time()

    # We must exclude one of the columns of the NYC dataset due to an unimplemented cast in Arrow.
    working_columns = [
        "vendor_id",
        "pickup_at",
        "dropoff_at",
        "passenger_count",
        "trip_distance",
        "pickup_longitude",
        "pickup_latitude",
        "store_and_fwd_flag",
        "dropoff_longitude",
        "dropoff_latitude",
        "payment_type",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "total_amount",
        "year",
        "month"
    ]

    # Open dataset using year,month folder partition
    nyc_dataset = ds.dataset('nyc-taxi/', partitioning=["year", "month"])
    # Generate a scanner to skip problematic column
    dataset_scanner = nyc_dataset.scanner(columns=working_columns)

    # Materialize dataset to an Arrow Table
    nyc_table = dataset_scanner.to_table()

    # Generate Dataframe from Arow Table
    nyc_df = nyc_table.to_pandas()

    # Apply Filter
    filtered_df = nyc_df[
        (nyc_df.total_amount > 100) &
        (nyc_df.year >2014)]

    # Apply Projection
    res = filtered_df[["total_amount", "passenger_count","year"]]

    # Transform Result back to an Arrow Table
    new_table = pa.Table.from_pandas(res)

    end = time.time()
    diff = end - start
    duration = float(end - start)
    write_result(f"benchmark_result", run, duration)

close_result()
