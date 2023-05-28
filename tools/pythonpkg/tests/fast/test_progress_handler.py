import duckdb
import pandas as pd
import numpy
import pytest
from datetime import date, timedelta

class MyState:
    def __init__(self):
        self.updates = []
        self.finished_count = 0

def scoped_handler(connection, state):
    def handler(state, progress, finished):
        state.updates.append(progress)
        if (finished):
            state.finished_count += 1
            print("FINISHED")
    
    connection.set_progress_handler(handler, state)

def long_running_query(con):
    df = pd.DataFrame({'i': numpy.arange(10000000)})
    df_2 = df
    #return con.execute("SELECT SUM(df.i) FROM df inner join df_2 on (df.i = df_2.i)").fetchall()
    print("BEFORE QUERY")
    res = con.execute(
        'select 42'
    ).fetchall()
    print("AFTER QUERY")
    print(res)

class TestProgressHandler(object):
    def test_progress_handler_basic(self, duckdb_cursor):
        def handler(state, progress, finished):
            state.updates.append(progress)
            if (finished):
                state.finished_count += 1
                print("FINISHED")
        
        con = duckdb.connect()
        con.execute("PRAGMA disable_print_progress_bar").fetchall()
        con.execute("PRAGMA progress_bar_time=0").fetchall()

        state = MyState()
        con.set_progress_handler(handler, state)
        print("SET HANDLER")

        # Long running query...
        res = long_running_query(con)

        # Check the state to see if the handler ran correctly.
        # maybe store a list of the progress updates, verify that the progress float updates are ascending
        print(state.updates)
        print(state.finished_count)

    #def test_scoped_handler(self):
    #    con = duckdb.connect()

    #    state = MyState()
    #    # Register a handler function that doesn't exist outside of this function
    #    # we have to keep a reference to this alive
    #    scoped_handler(con, state)

    #    # Long running query...
    #    res = long_running_query(con)

    #    # Check the state to see if the handler ran correctly.
    #    # maybe store a list of the progress updates, verify that the progress float updates are ascending
    #    print(state.updates)

