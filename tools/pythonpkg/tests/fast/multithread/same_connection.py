import duckdb
from threading import Thread, current_thread
import pandas as pd
import os
import pytest

def insert_from_same_connection(duckdb_cursor):
    # Insert a row with the name of the thread
    thread_name = str(current_thread().name)
    duckdb_cursor.execute("""INSERT INTO my_inserts VALUES (?)""", (thread_name,))

class TestSameConnection(object):
    def test_same_connection(self):
        duckdb_con = duckdb.connect() # In Memory DuckDB
        duckdb_con.execute("""CREATE OR REPLACE TABLE my_inserts (thread_name varchar)""")

        thread_count = 3
        threads = []
        cursors = []

        # Kick off multiple threads (in the same process) 
        # Pass in the same connection as an argument, and an object to store the results
        for i in range(thread_count):
            cursors.append(duckdb_con.cursor())
            threads.append(Thread(target=insert_from_same_connection,
                                    args=(cursors[i],),
                                    name='my_thread_'+str(i)))

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

        assert duckdb_con.execute("""SELECT * FROM my_inserts order by thread_name""").fetchall() == [('my_thread_0',), ('my_thread_1',), ('my_thread_2',)]
