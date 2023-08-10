import typing
from duckdb.typing import DuckDBPyType
from typing import List, Tuple, cast
from .types import (
    DataType,
    StringType,
    BinaryType,
    BitstringType,
    UUIDType,
    BooleanType,
    DateType,
    TimestampType,
    TimestampNTZType,
    TimeType,
    TimeNTZType,
    TimestampNanosecondNTZType,
    TimestampMilisecondNTZType,
    TimestampSecondNTZType,
    DecimalType,
    DoubleType,
    FloatType,
    ByteType,
    UnsignedByteType,
    ShortType,
    UnsignedShortType,
    IntegerType,
    UnsignedIntegerType,
    LongType,
    UnsignedLongType,
    HugeIntegerType,
    DayTimeIntervalType,
    ArrayType,
    MapType,
    StructField,
    StructType,
)

_sqltype_to_spark_class = {
    'boolean': BooleanType,
    'utinyint': UnsignedByteType,
    'tinyint': ByteType,
    'usmallint': UnsignedShortType,
    'smallint': ShortType,
    'uinteger': UnsignedIntegerType,
    'integer': IntegerType,
    'ubigint': UnsignedLongType,
    'bigint': LongType,
    'hugeint': HugeIntegerType,
    'varchar': StringType,
    'blob': BinaryType,
    'bit': BitstringType,
    'uuid': UUIDType,
    'date': DateType,
    'time': TimeNTZType,
    'time with time zone': TimeType,
    'timestamp': TimestampNTZType,
    'timestamp with time zone': TimestampType,
    'timestamp_ms': TimestampNanosecondNTZType,
    'timestamp_ns': TimestampMilisecondNTZType,
    'timestamp_s': TimestampSecondNTZType,
    'interval': DayTimeIntervalType,
    'list': ArrayType,
    'struct': StructType,
    'map': MapType,
    # union
    # enum
    # null (???)
    'float': FloatType,
    'double': DoubleType,
    'decimal': DecimalType,
}


def convert_nested_type(dtype: DuckDBPyType) -> DataType:
    id = dtype.id
    if id == 'list':
        children = dtype.children
        return ArrayType(convert_type(children[0][1]))
    # TODO: add support for 'union'
    if id == 'struct':
        children: List[Tuple[str, DuckDBPyType]] = dtype.children
        fields = [StructField(x[0], convert_type(x[1])) for x in children]
        return StructType(fields)
    if id == 'map':
        return MapType(convert_type(dtype.key), convert_type(dtype.value))
    raise NotImplementedError


def convert_type(dtype: DuckDBPyType) -> DataType:
    id = dtype.id
    if id in ['list', 'struct', 'map']:
        return convert_nested_type(dtype)
    if id == 'decimal':
        children: List[Tuple[str, DuckDBPyType]] = dtype.children
        precision = cast(int, children[0][1])
        scale = cast(int, children[1][1])
        return DecimalType(precision, scale)
    spark_type = _sqltype_to_spark_class[id]
    return spark_type()


def _parse_datatype_string(s: str) -> DataType:
    """
    Parses the given data type string to a :class:`DataType`. The data type string format equals
    :class:`DataType.simpleString`, except that the top level struct type can omit
    the ``struct<>``. Since Spark 2.3, this also supports a schema in a DDL-formatted
    string and case-insensitive strings.

    Examples
    --------
    >>> _parse_datatype_string("int ")
    IntegerType()
    >>> _parse_datatype_string("INT ")
    IntegerType()
    >>> _parse_datatype_string("a: byte, b: decimal(  16 , 8   ) ")
    StructType([StructField('a', ByteType(), True), StructField('b', DecimalType(16,8), True)])
    >>> _parse_datatype_string("a DOUBLE, b STRING")
    StructType([StructField('a', DoubleType(), True), StructField('b', StringType(), True)])
    >>> _parse_datatype_string("a DOUBLE, b CHAR( 50 )")
    StructType([StructField('a', DoubleType(), True), StructField('b', CharType(50), True)])
    >>> _parse_datatype_string("a DOUBLE, b VARCHAR( 50 )")
    StructType([StructField('a', DoubleType(), True), StructField('b', VarcharType(50), True)])
    >>> _parse_datatype_string("a: array< short>")
    StructType([StructField('a', ArrayType(ShortType(), True), True)])
    >>> _parse_datatype_string(" map<string , string > ")
    MapType(StringType(), StringType(), True)

    >>> # Error cases
    >>> _parse_datatype_string("blabla") # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    ParseException:...
    >>> _parse_datatype_string("a: int,") # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    ParseException:...
    >>> _parse_datatype_string("array<int") # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    ParseException:...
    >>> _parse_datatype_string("map<int, boolean>>") # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    ParseException:...
    """
    # TODO: this needs extra glue to transform Spark type syntax to equivalent types accepted by DuckDB
    # this will do for now:

    duckdb_type = DuckDBPyType(s)
    return convert_type(duckdb_type)

    # def from_ddl_schema(type_str: str) -> DataType:
    #    return _parse_datatype_json_string(
    #        cast(JVMView, sc._jvm).org.apache.spark.sql.types.StructType.fromDDL(type_str).json()
    #    )

    # def from_ddl_datatype(type_str: str) -> DataType:
    #    return _parse_datatype_json_string(
    #        cast(JVMView, sc._jvm)
    #        .org.apache.spark.sql.api.python.PythonSQLUtils.parseDataType(type_str)
    #        .json()
    #    )

    # try:
    #    # DDL format, "fieldname datatype, fieldname datatype".
    #    return from_ddl_schema(s)
    # except Exception as e:
    #    try:
    #        # For backwards compatibility, "integer", "struct<fieldname: datatype>" and etc.
    #        return from_ddl_datatype(s)
    #    except BaseException:
    #        try:
    #            # For backwards compatibility, "fieldname: datatype, fieldname: datatype" case.
    #            return from_ddl_datatype("struct<%s>" % s.strip())
    #        except BaseException:
    #            raise e


def duckdb_to_spark_schema(names: List[str], types: List[DuckDBPyType]) -> StructType:
    fields = [StructField(name, dtype) for name, dtype in zip(names, [convert_type(x) for x in types])]
    return StructType(fields)
