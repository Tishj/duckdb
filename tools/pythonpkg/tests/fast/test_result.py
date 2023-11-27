import duckdb
import pytest
import datetime


class TestPythonResult(object):
    def test_result_closed(self, duckdb_cursor):
        connection = duckdb.connect('')
        cursor = connection.cursor()
        cursor.execute('CREATE TABLE integers (i integer)')
        cursor.execute('INSERT INTO integers VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9),(NULL)')
        rel = connection.table("integers")
        res = rel.aggregate("sum(i)").execute()
        res.close()
        with pytest.raises(duckdb.InvalidInputException, match='Relation has already been closed'):
            res.fetchone()
        with pytest.raises(duckdb.InvalidInputException, match='Relation has already been closed'):
            res.fetchall()
        with pytest.raises(duckdb.InvalidInputException, match='Relation has already been closed'):
            res.fetchnumpy()
        with pytest.raises(duckdb.InvalidInputException, match='Relation has already been closed'):
            res.fetch_arrow_table()
        with pytest.raises(duckdb.InvalidInputException, match='Relation has already been closed'):
            res.fetch_arrow_reader(1)

    def test_result_describe_types(self, duckdb_cursor):
        connection = duckdb.connect('')
        cursor = connection.cursor()
        cursor.execute('CREATE TABLE test (i bool, j TIME, k VARCHAR)')
        cursor.execute("INSERT INTO test VALUES (TRUE, '01:01:01', 'bla' )")
        rel = connection.table("test")
        res = rel.execute()
        assert res.description == [
            ('i', 'bool', None, None, None, None, None),
            ('j', 'Time', None, None, None, None, None),
            ('k', 'STRING', None, None, None, None, None),
        ]

    def test_result_invalidated(self, duckdb_cursor):
        duckdb_cursor.execute(r'CREATE TABLE test(id INTEGER , name VARCHAR NOT NULL);')

        words = ['aaaaaaaaaaaaaaaaaaaaaaa', 'bbbb', 'ccccccccc', 'ííííííííí']
        lines = [(i, words[i % 4]) for i in range(1000)]
        duckdb_cursor.executemany("INSERT INTO TEST (id, name) VALUES (?, ?)", lines)

        rel1 = duckdb_cursor.sql(
            """
            SELECT id, name FROM test ORDER BY id DESC
        """
        )
        result = rel1.fetchmany(size=5)
        result = duckdb_cursor.sql("SELECT name from test order by id desc limit 1").fetchone()
        with pytest.raises(
            duckdb.InvalidInputException,
            match='This relation is no longer valid, it can no longer be fetched from. Try re-executing the relation',
        ):
            # On the same connection, it's not possible to fetch from one relation, then fetch from another, then attempt to continue fetching from the first relation
            # The internal result will be invalid
            result = rel1.fetchmany(size=5)

    def test_result_timestamps(self, duckdb_cursor):
        connection = duckdb.connect('')
        cursor = connection.cursor()
        cursor.execute(
            'CREATE TABLE IF NOT EXISTS timestamps (sec TIMESTAMP_S, milli TIMESTAMP_MS,micro TIMESTAMP_US, nano TIMESTAMP_NS );'
        )
        cursor.execute(
            "INSERT INTO timestamps VALUES ('2008-01-01 00:00:11','2008-01-01 00:00:01.794','2008-01-01 00:00:01.98926','2008-01-01 00:00:01.899268321' )"
        )

        rel = connection.table("timestamps")
        assert rel.execute().fetchall() == [
            (
                datetime.datetime(2008, 1, 1, 0, 0, 11),
                datetime.datetime(2008, 1, 1, 0, 0, 1, 794000),
                datetime.datetime(2008, 1, 1, 0, 0, 1, 989260),
                datetime.datetime(2008, 1, 1, 0, 0, 1, 899268),
            )
        ]

    def test_result_interval(self):
        connection = duckdb.connect()
        cursor = connection.cursor()
        cursor.execute('CREATE TABLE IF NOT EXISTS intervals (ivals INTERVAL)')
        cursor.execute("INSERT INTO intervals VALUES ('1 day'), ('2 second'), ('1 microsecond')")

        rel = connection.table("intervals")
        res = rel.execute()
        assert res.description == [('ivals', 'TIMEDELTA', None, None, None, None, None)]
        assert res.fetchall() == [
            (datetime.timedelta(days=1.0),),
            (datetime.timedelta(seconds=2.0),),
            (datetime.timedelta(microseconds=1.0),),
        ]

    def test_description_uuid(self):
        connection = duckdb.connect()
        connection.execute("select uuid();")
        connection.description
