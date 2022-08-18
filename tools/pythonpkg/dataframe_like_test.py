import duckdb
import polars as pl
import pandas as pd
import dask.dataframe as dd

duckdb.default_connection.register("yes", 5)

