# https://sparkbyexamples.com/pyspark/pyspark-udf-user-defined-function/

from tools.pythonpkg.pyduckdb.spark.exception import ContributionsAcceptedError
from ._typing import UserDefinedFunctionLike, DataTypeOrString
from typing import Callable, Optional, Any
from inspect import getfullargspec
import warnings
from .spark_user_defined_function import UserDefinedFunction


class UDFRegistration:
    def __init__(self):
        raise NotImplementedError


def _create_udf(
    f: Callable[..., Any],
    returnType: "DataTypeOrString",
    evalType: int,
    name: Optional[str] = None,
    deterministic: bool = True,
) -> "UserDefinedFunctionLike":
    """Create a regular(non-Arrow-optimized) Python UDF."""
    # Set the name of the UserDefinedFunction object to be the name of function f
    udf_obj = UserDefinedFunction(f, returnType=returnType, name=name, evalType=evalType, deterministic=deterministic)
    return udf_obj._wrapped()


def _create_py_udf(
    f: Callable[..., Any],
    returnType: "DataTypeOrString",
    useArrow: Optional[bool] = None,
) -> "UserDefinedFunctionLike":
    """Create a regular/Arrow-optimized Python UDF."""
    # The following table shows the results when the type coercion in Arrow is needed, that is,
    # when the user-specified return type(SQL Type) of the UDF and the actual instance(Python
    # Value(Type)) that the UDF returns are different.
    # Arrow and Pickle have different type coercion rules, so a UDF might have a different result
    # with/without Arrow optimization. That's the main reason the Arrow optimization for Python
    # UDFs is disabled by default.
    # +-----------------------------+--------------+----------+------+---------------+--------------------+-----------------------------+----------+----------------------+---------+--------------------+----------------------------+------------+--------------+  # noqa
    # |SQL Type \ Python Value(Type)|None(NoneType)|True(bool)|1(int)|         a(str)|    1970-01-01(date)|1970-01-01 00:00:00(datetime)|1.0(float)|array('i', [1])(array)|[1](list)|         (1,)(tuple)|bytearray(b'ABC')(bytearray)|  1(Decimal)|{'a': 1}(dict)|  # noqa
    # +-----------------------------+--------------+----------+------+---------------+--------------------+-----------------------------+----------+----------------------+---------+--------------------+----------------------------+------------+--------------+  # noqa
    # |                      boolean|          None|      True|  None|           None|                None|                         None|      None|                  None|     None|                None|                        None|        None|          None|  # noqa
    # |                      tinyint|          None|      None|     1|           None|                None|                         None|      None|                  None|     None|                None|                        None|        None|          None|  # noqa
    # |                     smallint|          None|      None|     1|           None|                None|                         None|      None|                  None|     None|                None|                        None|        None|          None|  # noqa
    # |                          int|          None|      None|     1|           None|                None|                         None|      None|                  None|     None|                None|                        None|        None|          None|  # noqa
    # |                       bigint|          None|      None|     1|           None|                None|                         None|      None|                  None|     None|                None|                        None|        None|          None|  # noqa
    # |                       string|          None|    'true'|   '1'|            'a'|'java.util.Gregor...|         'java.util.Gregor...|     '1.0'|         '[I@120d813a'|    '[1]'|'[Ljava.lang.Obje...|               '[B@48571878'|         '1'|       '{a=1}'|  # noqa
    # |                         date|          None|         X|     X|              X|datetime.date(197...|         datetime.date(197...|         X|                     X|        X|                   X|                           X|           X|             X|  # noqa
    # |                    timestamp|          None|         X|     X|              X|                   X|         datetime.datetime...|         X|                     X|        X|                   X|                           X|           X|             X|  # noqa
    # |                        float|          None|      None|  None|           None|                None|                         None|       1.0|                  None|     None|                None|                        None|        None|          None|  # noqa
    # |                       double|          None|      None|  None|           None|                None|                         None|       1.0|                  None|     None|                None|                        None|        None|          None|  # noqa
    # |                       binary|          None|      None|  None|bytearray(b'a')|                None|                         None|      None|                  None|     None|                None|           bytearray(b'ABC')|        None|          None|  # noqa
    # |                decimal(10,0)|          None|      None|  None|           None|                None|                         None|      None|                  None|     None|                None|                        None|Decimal('1')|          None|  # noqa
    # +-----------------------------+--------------+----------+------+---------------+--------------------+-----------------------------+----------+----------------------+---------+--------------------+----------------------------+------------+--------------+  # noqa
    # Note: Python 3.9.15, Pandas 1.5.2 and PyArrow 10.0.1 are used.
    # Note: The values of 'SQL Type' are DDL formatted strings, which can be used as `returnType`s.
    # Note: The values inside the table are generated by `repr`. X' means it throws an exception
    # during the conversion.

    if useArrow is None:
        # Spark tries to grab the setting from a global session's config
        is_arrow_enabled = False
    else:
        is_arrow_enabled = useArrow

    regular_udf = _create_udf(f, returnType, 2048)
    try:
        is_func_with_args = len(getfullargspec(f).args) > 0
    except TypeError:
        is_func_with_args = False
    if is_arrow_enabled:
        raise ContributionsAcceptedError
    else:
        return regular_udf


__all__ = ["UDFRegistration"]
