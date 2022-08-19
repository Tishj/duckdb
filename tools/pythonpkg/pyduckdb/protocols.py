from typing import Protocol, runtime_checkable

@runtime_checkable
class Placeholder(Protocol):
    def read(self):
        pass


@runtime_checkable
class TableLike(Protocol):
    def to_pydict(self):
        pass

    def slice(self, offset=0, length=None):
        pass

    def replace_schema_metadata(self, metadata=None):
        pass

    def to_batches(self, max_chunksize=None):
        pass

    def field(self, i):
        pass

    def remove_column(self, i: int):
        pass

    def column(self, i):
        pass

    def drop(self, columns):
        pass

    def itercolumns(self):
        pass

    def cast(self, target_schema, safe=True):
        pass

    def rename_columns(self, names):
        pass

    def equals(self, other, check_metadata=False):
        pass

    def add_column(self, i: int, field_, column):
        pass

    def set_column(self, i: int, field_, column):
        pass

    def __sizeof__(self):
        pass

    def combine_chunks(self, memory_pool=None):
        pass

    def filter(self, mask, null_selection_behavior=u'drop'):
        pass

    def append_column(self, field_, column):
        pass

    def flatten(self, memory_pool=None):
        pass

    def __reduce__(self):
        pass

#taken from Modin: https://github.com/modin-project/modin/blob/master/modin/_compat/pandas_api/abc/base.py
@runtime_checkable
class PandasDatasetLike(Protocol):
    """Interface for compatibility layer for Dataset."""

    def max(self, *args, **kwargs):  # noqa: GL08
        pass

    def min(self, *args, **kwargs):  # noqa: GL08
        pass

    def mean(self, *args, **kwargs):  # noqa: GL08
        pass

    def median(self, *args, **kwargs):  # noqa: GL08
        pass

    def rank(self, *args, **kwargs):  # noqa: GL08
        pass

    def reindex(self, *args, **kwargs):  # noqa: GL08
        pass

    def rolling(self, *args, **kwargs):  # noqa: GL08
        pass

    def sample(self, *args, **kwargs):  # noqa: GL08
        pass

    def sem(self, *args, **kwargs):  # noqa: GL08
        pass

    def shift(self, *args, **kwargs):  # noqa: GL08
        pass

    def skew(self, *args, **kwargs):  # noqa: GL08
        pass

    def std(self, *args, **kwargs):  # noqa: GL08
        pass

    def to_csv(self, *args, **kwargs):  # noqa: GL08
        pass

    def to_json(self, *args, **kwargs):  # noqa: GL08
        pass

    def to_markdown(self, *args, **kwargs):  # noqa: GL08
        pass

    def to_latex(self, *args, **kwargs):  # noqa: GL08
        pass

    def to_pickle(self, *args, **kwargs):  # noqa: GL08
        pass

    def var(self, *args, **kwargs):  # noqa: GL08
        pass

#taken from Modin: https://github.com/modin-project/modin/blob/master/modin/_compat/pandas_api/abc/dataframe.py

@runtime_checkable
class PandasDataFrameLike(Protocol):
    """Interface for compatibility layer for DataFrame."""

    def applymap(self, *args, **kwargs):  # noqa: GL08
        pass

    def apply(self, *args, **kwargs):  # noqa: GL08
        pass

    def info(self, *args, **kwargs):  # noqa: GL08
        pass

    def pivot_table(self, *args, **kwargs):  # noqa: GL08
        pass

    def prod(self, *args, **kwargs):  # noqa: GL08
        pass

    def replace(self, *args, **kwargs):  # noqa: GL08
        pass

    def sum(self, *args, **kwargs):  # noqa: GL08
        pass

    def to_parquet(self, *args, **kwargs):  # noqa: GL08
        pass
