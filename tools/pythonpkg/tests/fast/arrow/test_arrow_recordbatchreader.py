import duckdb
import os
import pytest

pyarrow = pytest.importorskip("pyarrow")
pyarrow.parquet = pytest.importorskip("pyarrow.parquet")
pyarrow.dataset = pytest.importorskip("pyarrow.dataset")
np = pytest.importorskip("numpy")


class TestArrowRecordBatchReader(object):
    def test_parallel_reader(self, duckdb_cursor):

        duckdb_conn = duckdb.connect()
        duckdb_conn.execute("PRAGMA threads=4")

        parquet_filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data', 'userdata1.parquet')

        userdata_parquet_dataset = pyarrow.dataset.dataset(
            [
                parquet_filename,
                parquet_filename,
                parquet_filename,
            ],
            format="parquet",
        )

        batches = [r for r in userdata_parquet_dataset.to_batches()]
        reader = pyarrow.dataset.Scanner.from_batches(batches, schema=userdata_parquet_dataset.schema).to_reader()

        rel = duckdb_conn.from_arrow(reader)

        assert (
            rel.filter("first_name=\'Jose\' and salary > 134708.82").aggregate('count(*)').execute().fetchone()[0] == 12
        )
        # The reader is already consumed so this should be 0
        assert (
            rel.filter("first_name=\'Jose\' and salary > 134708.82").aggregate('count(*)').execute().fetchone()[0] == 0
        )

    def test_parallel_reader_replacement_scans(self, duckdb_cursor):

        duckdb_conn = duckdb.connect()
        duckdb_conn.execute("PRAGMA threads=4")

        parquet_filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data', 'userdata1.parquet')

        userdata_parquet_dataset = pyarrow.dataset.dataset(
            [
                parquet_filename,
                parquet_filename,
                parquet_filename,
            ],
            format="parquet",
        )

        batches = [r for r in userdata_parquet_dataset.to_batches()]
        reader = pyarrow.dataset.Scanner.from_batches(batches, schema=userdata_parquet_dataset.schema).to_reader()

        assert (
            duckdb_conn.execute(
                "select count(*) r1 from reader where first_name=\'Jose\' and salary > 134708.82"
            ).fetchone()[0]
            == 12
        )
        with pytest.raises(
            duckdb.InvalidInputException, match='Attempted to read from the same RecordBatchReader more than once!'
        ):
            duckdb_conn.execute(
                "select count(*) r2 from reader where first_name=\'Jose\' and salary > 134708.82"
            ).fetchone()

    def test_parallel_reader_register(self, duckdb_cursor):

        duckdb_conn = duckdb.connect()
        duckdb_conn.execute("PRAGMA threads=4")

        parquet_filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data', 'userdata1.parquet')

        userdata_parquet_dataset = pyarrow.dataset.dataset(
            [
                parquet_filename,
                parquet_filename,
                parquet_filename,
            ],
            format="parquet",
        )

        batches = [r for r in userdata_parquet_dataset.to_batches()]
        reader = pyarrow.dataset.Scanner.from_batches(batches, schema=userdata_parquet_dataset.schema).to_reader()

        duckdb_conn.register("bla", reader)

        assert (
            duckdb_conn.execute("select count(*) from bla where first_name=\'Jose\' and salary > 134708.82").fetchone()[
                0
            ]
            == 12
        )
        assert (
            duckdb_conn.execute("select count(*) from bla where first_name=\'Jose\' and salary > 134708.82").fetchone()[
                0
            ]
            == 0
        )

    def test_parallel_reader_default_conn(self, duckdb_cursor):

        parquet_filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data', 'userdata1.parquet')

        userdata_parquet_dataset = pyarrow.dataset.dataset(
            [
                parquet_filename,
                parquet_filename,
                parquet_filename,
            ],
            format="parquet",
        )

        batches = [r for r in userdata_parquet_dataset.to_batches()]
        reader = pyarrow.dataset.Scanner.from_batches(batches, schema=userdata_parquet_dataset.schema).to_reader()

        rel = duckdb.from_arrow(reader)

        assert (
            rel.filter("first_name=\'Jose\' and salary > 134708.82").aggregate('count(*)').execute().fetchone()[0] == 12
        )
        # The reader is already consumed so this should be 0
        assert (
            rel.filter("first_name=\'Jose\' and salary > 134708.82").aggregate('count(*)').execute().fetchone()[0] == 0
        )

    def test_arrow_pivot_error(self, duckdb_cursor):
        # Create a simple Arrow RecordBatch and associated reader
        batch = pyarrow.RecordBatch.from_pydict({'name': ["one", "two", "three", "four"], 'value': [1, 2, 3, 4]})
        rbr = pyarrow.RecordBatchReader.from_batches(schema=batch.schema, batches=[batch])

        with pytest.raises(
            duckdb.InvalidInputException, match='Attempted to read from the same RecordBatchReader more than once!'
        ):
            # PIVOT issues multiple queries, attempting to read from the same source more than once
            rel = duckdb_cursor.sql('PIVOT rbr ON name USING FIRST(value)')
