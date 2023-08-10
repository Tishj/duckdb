from .column import Column
from typing import Any, overload, Optional, Dict, cast, Union, Callable

from duckdb import CaseExpression, ConstantExpression, ColumnExpression, FunctionExpression, Expression
from ._typing import ColumnOrName, DataTypeOrString, UserDefinedFunctionLike

from .types import StringType, DataType
from functools import partial


def _invoke_function_over_columns(name: str, *cols: "ColumnOrName") -> Column:
    """
    Invokes n-ary JVM function identified by name
    and wraps the result with :class:`~pyspark.sql.Column`.
    """
    cols = [_to_column(expr) for expr in cols]
    return _invoke_function(name, *cols)


def col(column: str):
    return Column(ColumnExpression(column))


def when(condition: "Column", value: Any) -> Column:
    if not isinstance(condition, Column):
        raise TypeError("condition should be a Column")
    v = value.expr if isinstance(value, Column) else value
    expr = CaseExpression(condition.expr, v)
    return Column(expr)


def _inner_expr_or_val(val):
    return val.expr if isinstance(val, Column) else val


def struct(*cols: Column) -> Column:
    return Column(FunctionExpression('struct_pack', *[_inner_expr_or_val(x) for x in cols]))


def lit(col: Any) -> Column:
    return col if isinstance(col, Column) else Column(ConstantExpression(col))


def _invoke_function(function: str, *arguments):
    return Column(FunctionExpression(function, *arguments))


def _to_column(col: ColumnOrName) -> Expression:
    return col.expr if isinstance(col, Column) else ColumnExpression(col)


def regexp_replace(str: "ColumnOrName", pattern: str, replacement: str) -> Column:
    r"""Replace all substrings of the specified string value that match regexp with rep.

    .. versionadded:: 1.5.0

    Examples
    --------
    >>> df = spark.createDataFrame([('100-200',)], ['str'])
    >>> df.select(regexp_replace('str', r'(\d+)', '--').alias('d')).collect()
    [Row(d='-----')]
    """
    return _invoke_function("regexp_replace", _to_column(str), pattern, replacement)


def array_contains(col: "ColumnOrName", value: Any) -> Column:
    """
    Collection function: returns null if the array is null, true if the array contains the
    given value, and false otherwise.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        name of column containing array
    value :
        value or column to check for in array

    Returns
    -------
    :class:`~pyspark.sql.Column`
        a column of Boolean type.

    Examples
    --------
    >>> df = spark.createDataFrame([(["a", "b", "c"],), ([],)], ['data'])
    >>> df.select(array_contains(df.data, "a")).collect()
    [Row(array_contains(data, a)=True), Row(array_contains(data, a)=False)]
    >>> df.select(array_contains(df.data, lit("a"))).collect()
    [Row(array_contains(data, a)=True), Row(array_contains(data, a)=False)]
    """
    value = value.expr if isinstance(value, Column) else value
    return _invoke_function("array_contains", _to_column(col), value)


def avg(col: "ColumnOrName") -> Column:
    """
    Aggregate function: returns the average of the values in a group.

    .. versionadded:: 1.3.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to compute on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the column for computed results.

    Examples
    --------
    >>> df = spark.range(10)
    >>> df.select(avg(col("id"))).show()
    +-------+
    |avg(id)|
    +-------+
    |    4.5|
    +-------+
    """
    return _invoke_function_over_columns("avg", col)


def sum(col: "ColumnOrName") -> Column:
    """
    Aggregate function: returns the sum of all values in the expression.

    .. versionadded:: 1.3.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to compute on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the column for computed results.

    Examples
    --------
    >>> df = spark.range(10)
    >>> df.select(sum(df["id"])).show()
    +-------+
    |sum(id)|
    +-------+
    |     45|
    +-------+
    """
    return _invoke_function_over_columns("sum", col)


def max(col: "ColumnOrName") -> Column:
    """
    Aggregate function: returns the maximum value of the expression in a group.

    .. versionadded:: 1.3.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to compute on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        column for computed results.

    Examples
    --------
    >>> df = spark.range(10)
    >>> df.select(max(col("id"))).show()
    +-------+
    |max(id)|
    +-------+
    |      9|
    +-------+
    """
    return _invoke_function_over_columns("max", col)


def mean(col: "ColumnOrName") -> Column:
    """
    Aggregate function: returns the average of the values in a group.
    An alias of :func:`avg`.

    .. versionadded:: 1.4.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to compute on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the column for computed results.

    Examples
    --------
    >>> df = spark.range(10)
    >>> df.select(mean(df.id)).show()
    +-------+
    |avg(id)|
    +-------+
    |    4.5|
    +-------+
    """
    return _invoke_function_over_columns("mean", col)


def min(col: "ColumnOrName") -> Column:
    """
    Aggregate function: returns the minimum value of the expression in a group.

    .. versionadded:: 1.3.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to compute on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        column for computed results.

    Examples
    --------
    >>> df = spark.range(10)
    >>> df.select(min(df.id)).show()
    +-------+
    |min(id)|
    +-------+
    |      0|
    +-------+
    """
    return _invoke_function_over_columns("min", col)


def count(col: "ColumnOrName") -> Column:
    """
    Aggregate function: returns the number of items in a group.

    .. versionadded:: 1.3.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or str
        target column to compute on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        column for computed results.

    Examples
    --------
    Count by all columns (start), and by a column that does not count ``None``.

    >>> df = spark.createDataFrame([(None,), ("a",), ("b",), ("c",)], schema=["alphabets"])
    >>> df.select(count(expr("*")), count(df.alphabets)).show()
    +--------+----------------+
    |count(1)|count(alphabets)|
    +--------+----------------+
    |       4|               3|
    +--------+----------------+
    """
    return _invoke_function_over_columns("count", col)


@overload
def udf(
    f: Callable[..., Any],
    returnType: "DataTypeOrString" = StringType(),
    *,
    useArrow: Optional[bool] = None,
) -> "UserDefinedFunctionLike":
    ...


@overload
def udf(
    f: Optional["DataTypeOrString"] = None,
    *,
    useArrow: Optional[bool] = None,
) -> Callable[[Callable[..., Any]], "UserDefinedFunctionLike"]:
    ...


@overload
def udf(
    *,
    returnType: "DataTypeOrString" = StringType(),
    useArrow: Optional[bool] = None,
) -> Callable[[Callable[..., Any]], "UserDefinedFunctionLike"]:
    ...


def udf(
    f: Optional[Union[Callable[..., Any], "DataTypeOrString"]] = None,
    returnType: "DataTypeOrString" = StringType(),
    *,
    useArrow: Optional[bool] = None,
) -> Union["UserDefinedFunctionLike", Callable[[Callable[..., Any]], "UserDefinedFunctionLike"]]:
    """Creates a user defined function (UDF).

    .. versionadded:: 1.3.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    f : function
        python function if used as a standalone function
    returnType : :class:`pyspark.sql.types.DataType` or str
        the return type of the user-defined function. The value can be either a
        :class:`pyspark.sql.types.DataType` object or a DDL-formatted type string.
    useArrow : bool or None
        whether to use Arrow to optimize the (de)serialization. When it is None, the
        Spark config "spark.sql.execution.pythonUDF.arrow.enabled" takes effect.

    Examples
    --------
    >>> from pyspark.sql.types import IntegerType
    >>> slen = udf(lambda s: len(s), IntegerType())
    >>> @udf
    ... def to_upper(s):
    ...     if s is not None:
    ...         return s.upper()
    ...
    >>> @udf(returnType=IntegerType())
    ... def add_one(x):
    ...     if x is not None:
    ...         return x + 1
    ...
    >>> df = spark.createDataFrame([(1, "John Doe", 21)], ("id", "name", "age"))
    >>> df.select(slen("name").alias("slen(name)"), to_upper("name"), add_one("age")).show()
    +----------+--------------+------------+
    |slen(name)|to_upper(name)|add_one(age)|
    +----------+--------------+------------+
    |         8|      JOHN DOE|          22|
    +----------+--------------+------------+

    Notes
    -----
    The user-defined functions are considered deterministic by default. Due to
    optimization, duplicate invocations may be eliminated or the function may even be invoked
    more times than it is present in the query. If your function is not deterministic, call
    `asNondeterministic` on the user defined function. E.g.:

    >>> from pyspark.sql.types import IntegerType
    >>> import random
    >>> random_udf = udf(lambda: int(random.random() * 100), IntegerType()).asNondeterministic()

    The user-defined functions do not support conditional expressions or short circuiting
    in boolean expressions and it ends up with being executed all internally. If the functions
    can fail on special rows, the workaround is to incorporate the condition into the functions.

    The user-defined functions do not take keyword arguments on the calling side.
    """

    # The following table shows most of Python data and SQL type conversions in normal UDFs that
    # are not yet visible to the user. Some of behaviors are buggy and might be changed in the near
    # future. The table might have to be eventually documented externally.
    # Please see SPARK-28131's PR to see the codes in order to generate the table below.
    #
    # +-----------------------------+--------------+----------+------+---------------+--------------------+-----------------------------+----------+----------------------+---------+--------------------+----------------------------+------------+--------------+------------------+----------------------+  # noqa
    # |SQL Type \ Python Value(Type)|None(NoneType)|True(bool)|1(int)|         a(str)|    1970-01-01(date)|1970-01-01 00:00:00(datetime)|1.0(float)|array('i', [1])(array)|[1](list)|         (1,)(tuple)|bytearray(b'ABC')(bytearray)|  1(Decimal)|{'a': 1}(dict)|Row(kwargs=1)(Row)|Row(namedtuple=1)(Row)|  # noqa
    # +-----------------------------+--------------+----------+------+---------------+--------------------+-----------------------------+----------+----------------------+---------+--------------------+----------------------------+------------+--------------+------------------+----------------------+  # noqa
    # |                      boolean|          None|      True|  None|           None|                None|                         None|      None|                  None|     None|                None|                        None|        None|          None|                 X|                     X|  # noqa
    # |                      tinyint|          None|      None|     1|           None|                None|                         None|      None|                  None|     None|                None|                        None|        None|          None|                 X|                     X|  # noqa
    # |                     smallint|          None|      None|     1|           None|                None|                         None|      None|                  None|     None|                None|                        None|        None|          None|                 X|                     X|  # noqa
    # |                          int|          None|      None|     1|           None|                None|                         None|      None|                  None|     None|                None|                        None|        None|          None|                 X|                     X|  # noqa
    # |                       bigint|          None|      None|     1|           None|                None|                         None|      None|                  None|     None|                None|                        None|        None|          None|                 X|                     X|  # noqa
    # |                       string|          None|    'true'|   '1'|            'a'|'java.util.Gregor...|         'java.util.Gregor...|     '1.0'|         '[I@66cbb73a'|    '[1]'|'[Ljava.lang.Obje...|               '[B@5a51eb1a'|         '1'|       '{a=1}'|                 X|                     X|  # noqa
    # |                         date|          None|         X|     X|              X|datetime.date(197...|         datetime.date(197...|         X|                     X|        X|                   X|                           X|           X|             X|                 X|                     X|  # noqa
    # |                    timestamp|          None|         X|     X|              X|                   X|         datetime.datetime...|         X|                     X|        X|                   X|                           X|           X|             X|                 X|                     X|  # noqa
    # |                        float|          None|      None|  None|           None|                None|                         None|       1.0|                  None|     None|                None|                        None|        None|          None|                 X|                     X|  # noqa
    # |                       double|          None|      None|  None|           None|                None|                         None|       1.0|                  None|     None|                None|                        None|        None|          None|                 X|                     X|  # noqa
    # |                   array<int>|          None|      None|  None|           None|                None|                         None|      None|                   [1]|      [1]|                 [1]|                [65, 66, 67]|        None|          None|                 X|                     X|  # noqa
    # |                       binary|          None|      None|  None|bytearray(b'a')|                None|                         None|      None|                  None|     None|                None|           bytearray(b'ABC')|        None|          None|                 X|                     X|  # noqa
    # |                decimal(10,0)|          None|      None|  None|           None|                None|                         None|      None|                  None|     None|                None|                        None|Decimal('1')|          None|                 X|                     X|  # noqa
    # |              map<string,int>|          None|      None|  None|           None|                None|                         None|      None|                  None|     None|                None|                        None|        None|      {'a': 1}|                 X|                     X|  # noqa
    # |               struct<_1:int>|          None|         X|     X|              X|                   X|                            X|         X|                     X|Row(_1=1)|           Row(_1=1)|                           X|           X|  Row(_1=None)|         Row(_1=1)|             Row(_1=1)|  # noqa
    # +-----------------------------+--------------+----------+------+---------------+--------------------+-----------------------------+----------+----------------------+---------+--------------------+----------------------------+------------+--------------+------------------+----------------------+  # noqa
    #
    # Note: DDL formatted string is used for 'SQL Type' for simplicity. This string can be
    #       used in `returnType`.
    # Note: The values inside of the table are generated by `repr`.
    # Note: 'X' means it throws an exception during the conversion.

    # decorator @udf, @udf(), @udf(dataType())
    if f is None or isinstance(f, (str, DataType)):
        # If DataType has been passed as a positional argument
        # for decorator use it as a returnType
        return_type = f or returnType
        return partial(
            _create_py_udf,
            returnType=return_type,
            useArrow=useArrow,
        )
    else:
        return _create_py_udf(f=f, returnType=returnType, useArrow=useArrow)
