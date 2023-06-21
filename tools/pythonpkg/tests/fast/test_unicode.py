#!/usr/bin/env python
# -*- coding: utf-8 -*-

import duckdb
import pandas as pd

class TestUnicode(object):
    def test_unicode_pandas_scan(self, duckdb_cursor):
        con = duckdb.connect(database=':memory:', read_only=False)
        test_df = pd.DataFrame.from_dict({"i":[1, 2, 3], "j":["a", "c", u"ë"]})
        con.register('test_df_view', test_df)
        con.execute('SELECT i, j, LENGTH(j) FROM test_df_view').fetchall()

    def test_unicode_types(self):
        con = duckdb.connect()
        
        test_df = pd.DataFrame.from_dict({
            'ascii': [chr(56)],
            'ascii_min': [chr(0)],
            'ascii_max': [chr(127)],
            '1byte_utf8': [chr(156)],
            '1byte_utf8_min': [chr(128)],
            '1byte_utf8_max': [chr(255)],
            '2byte_utf8': [chr(425)],
            '2byte_utf8_min': [chr(256)],
            '2byte_utf8_max': [chr(2047)],
            '3byte_utf8': [chr(34532)],
            '3byte_utf8_min': [chr(2048)],
            '3byte_utf8_max': [chr(65535)],
            '4byte_utf8': [chr(234234)],
            '4byte_utf8_min': [chr(65536)],
            '4byte_utf8_max': [chr(1114111)]
        })
        con.register('test_df_view', test_df)
        converted_df = con.execute('select * from test_df_view').df()
        #expected = test_df['1byte_utf8'][0]
        #print(bytes(expected, encoding='utf8'))
        #print(len(bytes(expected, encoding='utf8')))
        #actual = converted_df['1byte_utf8'][0]
        #print(bytes(actual, encoding='utf8'))
        #print(len(bytes(actual, encoding='utf8')))
        pd.testing.assert_frame_equal(test_df, converted_df)
