#!/usr/bin/env python
# -*- coding: utf-8 -*-

import duckdb
import pandas as pd
import pytest

class TestUnicode(object):
    def test_unicode_pandas_scan(self, duckdb_cursor):
        con = duckdb.connect(database=':memory:', read_only=False)
        test_df = pd.DataFrame.from_dict({"i":[1, 2, 3], "j":["a", "c", u"ë"]})
        con.register('test_df_view', test_df)
        con.execute('SELECT i, j, LENGTH(j) FROM test_df_view').fetchall()

    @pytest.mark.parametrize('row_count', [1, 10, 5000])
    def test_unicode_types(self, row_count):
        con = duckdb.connect()
        
        test_df = pd.DataFrame.from_dict({
            'ascii': [chr(56) for _ in range(row_count)],
            'ascii_min': [chr(0) for _ in range(row_count)],
            'ascii_max': [chr(127) for _ in range(row_count)],
            '1byte_utf8': [chr(156) for _ in range(row_count)],
            '1byte_utf8_min': [chr(128) for _ in range(row_count)],
            '1byte_utf8_max': [chr(255) for _ in range(row_count)],
            '2byte_utf8': [chr(425) for _ in range(row_count)],
            '2byte_utf8_min': [chr(256) for _ in range(row_count)],
            '2byte_utf8_max': [chr(2047) for _ in range(row_count)],
            '3byte_utf8': [chr(34532) for _ in range(row_count)],
            '3byte_utf8_min': [chr(2048) for _ in range(row_count)],
            '3byte_utf8_max': [chr(65535) for _ in range(row_count)],
            '4byte_utf8': [chr(234234) for _ in range(row_count)],
            '4byte_utf8_min': [chr(65536) for _ in range(row_count)],
            '4byte_utf8_max': [chr(1114111) for _ in range(row_count)]
        })
        con.register('test_df_view', test_df)
        converted_df = con.execute('select * from test_df_view').df()

        pd.testing.assert_frame_equal(test_df, converted_df)
