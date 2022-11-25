import duckdb
import os
import pytest

class TestArrowScanner(object):

    def test_parallel_scanner(self,duckdb_cursor):
        pyarrow = pytest.importorskip("pyarrow")
        pc = pytest.importorskip("pyarrow.compute")

        duckdb_conn = duckdb.connect()
        duckdb_conn.execute("PRAGMA threads=4")

        parquet_filename = os.path.join(os.path.dirname(os.path.realpath(__file__)),'data','userdata1.parquet')

        arrow_dataset= pyarrow.dataset.dataset([
            parquet_filename,
            parquet_filename,
            parquet_filename,
        ]
        , format="parquet")

        scanner_filter = (pc.field("first_name") == pc.scalar('Jose')) & (pc.field("salary") > pc.scalar(134708.82))

        arrow_scanner = pyarrow.dataset.Scanner.from_dataset(arrow_dataset, filter=scanner_filter)

        rel = duckdb_conn.from_arrow(arrow_scanner)

        assert rel.aggregate('count(*)').execute().fetchone()[0] == 12

    def test_parallel_scanner_replacement_scans(self,duckdb_cursor):
        pyarrow = pytest.importorskip("pyarrow")
        pc = pytest.importorskip("pyarrow.compute")

        duckdb_conn = duckdb.connect()
        duckdb_conn.execute("PRAGMA threads=4")

        parquet_filename = os.path.join(os.path.dirname(os.path.realpath(__file__)),'data','userdata1.parquet')

        arrow_dataset= pyarrow.dataset.dataset([
            parquet_filename,
            parquet_filename,
            parquet_filename,
        ]
        , format="parquet")

        scanner_filter = (pc.field("first_name") == pc.scalar('Jose')) & (pc.field("salary") > pc.scalar(134708.82))

        arrow_scanner = pyarrow.dataset.Scanner.from_dataset(arrow_dataset, filter=scanner_filter)

        assert duckdb_conn.execute("select count(*) from arrow_scanner").fetchone()[0] == 12


    def test_parallel_scanner_register(self,duckdb_cursor):
        pyarrow = pytest.importorskip("pyarrow")
        pc = pytest.importorskip("pyarrow.compute")

        duckdb_conn = duckdb.connect()
        duckdb_conn.execute("PRAGMA threads=4")

        parquet_filename = os.path.join(os.path.dirname(os.path.realpath(__file__)),'data','userdata1.parquet')

        arrow_dataset= pyarrow.dataset.dataset([
            parquet_filename,
            parquet_filename,
            parquet_filename,
        ]
        , format="parquet")

        scanner_filter = (pc.field("first_name") == pc.scalar('Jose')) & (pc.field("salary") > pc.scalar(134708.82))

        arrow_scanner = pyarrow.dataset.Scanner.from_dataset(arrow_dataset, filter=scanner_filter)

        duckdb_conn.register("bla", arrow_scanner)

        assert duckdb_conn.execute("select count(*) from bla").fetchone()[0] == 12

    def test_parallel_scanner_default_conn(self,duckdb_cursor):
        pyarrow = pytest.importorskip("pyarrow")
        pc = pytest.importorskip("pyarrow.compute")

        parquet_filename = os.path.join(os.path.dirname(os.path.realpath(__file__)),'data','userdata1.parquet')

        arrow_dataset= pyarrow.dataset.dataset([
            parquet_filename,
            parquet_filename,
            parquet_filename,
        ]
        , format="parquet")

        scanner_filter = (pc.field("first_name") == pc.scalar('Jose')) & (pc.field("salary") > pc.scalar(134708.82))

        arrow_scanner = pyarrow.dataset.Scanner.from_dataset(arrow_dataset, filter=scanner_filter)

        rel = duckdb.from_arrow(arrow_scanner)

        assert rel.aggregate('count(*)').execute().fetchone()[0] == 12
