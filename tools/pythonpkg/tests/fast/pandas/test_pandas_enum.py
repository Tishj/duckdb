import pandas as pd
import pytest
import duckdb

class TestPandasEnum(object):
    def test_3480(self, duckdb_cursor):
        duckdb_cursor.execute(
        """
        create type cat as enum ('marie', 'duchess', 'toulouse');
        create table tab (
            cat cat,
            amt int
        );
        """
        )
        df = duckdb_cursor.query(f"SELECT * FROM tab LIMIT 0;").to_df()
        assert df["cat"].cat.categories.equals(pd.Index(['marie', 'duchess', 'toulouse']))
        duckdb_cursor.execute("DROP TABLE tab")
        duckdb_cursor.execute("DROP TYPE cat")

    def test_3479(self, duckdb_cursor):
        duckdb_cursor.execute(
        """
        create type cat as enum ('marie', 'duchess', 'toulouse');
        create table tab (
            cat cat,
            amt int
        );
        """
        )

        df = pd.DataFrame({"cat2": pd.Series(['duchess', 'toulouse', 'marie', None, "berlioz", "o_malley"], dtype="category"), "amt": [1, 2, 3, 4, 5, 6]})
        duckdb_cursor.register('df', df)
        with pytest.raises(duckdb.ConversionException, match='Type UINT8 with value 0 can\'t be cast because the value is out of range for the destination type UINT8'):
            duckdb_cursor.execute(f"INSERT INTO tab SELECT * FROM df;")

        assert duckdb_cursor.execute("select * from tab").fetchall() == []
        duckdb_cursor.execute("DROP TABLE tab")
        duckdb_cursor.execute("DROP TYPE cat")
    
    def test_enum_comparisons(self, duckdb_cursor):
        pd = pytest.importorskip("pandas")
        con = duckdb.connect()
        
        df = pd.DataFrame({
            "col1" : pd.Categorical(pd.Series(["a", "b", "c"]))
        })
        con.execute("""
	        create table tbl as select * from df
        """)

        res = con.table('tbl')
        print(res)

        # Can not insert an invalid value into an ENUM column
        with pytest.raises(duckdb.ConversionException):
            res = con.sql("""
                insert into tbl VALUES ('test')
            """)

        # this does work
        res = con.sql(
            "select * from tbl where col1 IN ('test')"
        ).fetchall()
        assert res == []

        res = con.sql(
            "select * from tbl where col1 NOT IN ('test')"
        ).fetchall()
        assert res == [('a',), ('b',), ('c',)]