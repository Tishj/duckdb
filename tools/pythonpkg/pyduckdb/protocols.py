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
