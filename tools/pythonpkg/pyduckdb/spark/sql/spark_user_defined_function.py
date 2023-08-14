from tools.pythonpkg.pyduckdb.spark.exception import ContributionsAcceptedError
from ._typing import DataTypeOrString, ColumnOrName, UserDefinedFunctionLike
from .types import StringType, DataType, StructType
from .type_utils import _parse_datatype_string
from typing import Callable, Optional, Any
from column import Column
import warnings
import functools
import inspect  # signate, getsourcelines

# import pyspark.profiler Profiler missing


class UserDefinedFunction:
    """
    User defined function in Python

    .. versionadded:: 1.3

    Notes
    -----
    The constructor of this class is not supposed to be directly called.
    Use :meth:`pyspark.sql.functions.udf` or :meth:`pyspark.sql.functions.pandas_udf`
    to create this instance.
    """

    def __init__(
        self,
        func: Callable[..., Any],
        returnType: "DataTypeOrString" = StringType(),
        name: Optional[str] = None,
        evalType: int = 100,
        deterministic: bool = True,
    ):
        if not callable(func):
            raise TypeError("NOT_CALLABLE")

        if not isinstance(returnType, (DataType, str)):
            raise TypeError("NOT_DATATYPE_OR_STR")

        if not isinstance(evalType, int):
            raise TypeError("NOT_INT")

        self.func = func
        self._returnType = returnType
        # Stores UserDefinedPythonFunctions jobj, once initialized
        self._returnType_placeholder: Optional[DataType] = None
        self.duck_udf_placeholder = None
        self._name = name or (func.__name__ if hasattr(func, "__name__") else func.__class__.__name__)
        self.evalType = evalType
        self.deterministic = deterministic

    @property
    def returnType(self) -> DataType:
        # This makes sure this is called after SparkContext is initialized.
        # ``_parse_datatype_string`` accesses to JVM for parsing a DDL formatted string.
        # TODO: PythonEvalType.SQL_BATCHED_UDF
        if self._returnType_placeholder is None:
            if isinstance(self._returnType, DataType):
                self._returnType_placeholder = self._returnType
            else:
                self._returnType_placeholder = _parse_datatype_string(self._returnType)

        if (
            self.evalType == PythonEvalType.SQL_SCALAR_PANDAS_UDF
            or self.evalType == PythonEvalType.SQL_SCALAR_PANDAS_ITER_UDF
        ):
            try:
                to_arrow_type(self._returnType_placeholder)
            except TypeError:
                raise NotImplementedError("NOT_IMPLEMENTED")
        elif (
            self.evalType == PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF
            or self.evalType == PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF_WITH_STATE
        ):
            if isinstance(self._returnType_placeholder, StructType):
                try:
                    to_arrow_type(self._returnType_placeholder)
                except TypeError:
                    raise NotImplementedError("NOT_IMPLEMENTED")
            else:
                raise TypeError("INVALID_RETURN_TYPE_FOR_PANDAS_UDF")
        elif (
            self.evalType == PythonEvalType.SQL_MAP_PANDAS_ITER_UDF
            or self.evalType == PythonEvalType.SQL_MAP_ARROW_ITER_UDF
        ):
            if isinstance(self._returnType_placeholder, StructType):
                try:
                    to_arrow_type(self._returnType_placeholder)
                except TypeError:
                    raise NotImplementedError("NOT_IMPLEMENTED")
            else:
                raise TypeError("INVALID_RETURN_TYPE_FOR_PANDAS_UDF")
        elif self.evalType == PythonEvalType.SQL_COGROUPED_MAP_PANDAS_UDF:
            if isinstance(self._returnType_placeholder, StructType):
                try:
                    to_arrow_type(self._returnType_placeholder)
                except TypeError:
                    raise NotImplementedError("NOT_IMPLEMENTED")
            else:
                raise TypeError("INVALID_RETURN_TYPE_FOR_PANDAS_UDF")
        elif self.evalType == PythonEvalType.SQL_GROUPED_AGG_PANDAS_UDF:
            try:
                # StructType is not yet allowed as a return type, explicitly check here to fail fast
                if isinstance(self._returnType_placeholder, StructType):
                    raise TypeError
                to_arrow_type(self._returnType_placeholder)
            except TypeError:
                raise NotImplementedError("NOT_IMPLEMENTED")

        return self._returnType_placeholder

    def __call__(self, *cols: "ColumnOrName") -> Column:
        raise ContributionsAcceptedError

    # This function is for improving the online help system in the interactive interpreter.
    # For example, the built-in help / pydoc.help. It wraps the UDF with the docstring and
    # argument annotation. (See: SPARK-19161)
    def _wrapped(self) -> "UserDefinedFunctionLike":
        """
        Wrap this udf with a function and attach docstring from func
        """

        # It is possible for a callable instance without __name__ attribute or/and
        # __module__ attribute to be wrapped here. For example, functools.partial. In this case,
        # we should avoid wrapping the attributes from the wrapped function to the wrapper
        # function. So, we take out these attribute names from the default names to set and
        # then manually assign it after being wrapped.
        assignments = tuple(a for a in functools.WRAPPER_ASSIGNMENTS if a != "__name__" and a != "__module__")

        @functools.wraps(self.func, assigned=assignments)
        def wrapper(*args: "ColumnOrName") -> Column:
            return self(*args)

        wrapper.__name__ = self._name
        wrapper.__module__ = (
            self.func.__module__ if hasattr(self.func, "__module__") else self.func.__class__.__module__
        )

        wrapper.func = self.func  # type: ignore[attr-defined]
        wrapper.returnType = self.returnType  # type: ignore[attr-defined]
        wrapper.evalType = self.evalType  # type: ignore[attr-defined]
        wrapper.deterministic = self.deterministic  # type: ignore[attr-defined]
        wrapper.asNondeterministic = functools.wraps(self.asNondeterministic)(  # type: ignore[attr-defined]
            lambda: self.asNondeterministic()._wrapped()
        )
        wrapper._unwrapped = self  # type: ignore[attr-defined]
        return wrapper  # type: ignore[return-value]

    def asNondeterministic(self) -> "UserDefinedFunction":
        """
        Updates UserDefinedFunction to nondeterministic.

        .. versionadded:: 2.3
        """
        # Here, we explicitly clean the cache to create a JVM UDF instance
        # with 'deterministic' updated. See SPARK-23233.
        self.duck_udf_placeholder = None
        self.deterministic = False
        return self
