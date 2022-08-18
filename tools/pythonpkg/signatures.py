['to_pydict', 'slice', '__ge__', 'replace_schema_metadata', '__ne__', '__delattr__', 'shape', '__format__', '__repr__', '__new__', 'to_batches', '__le__', '__getattribute__', 'validate', 'field', '__len__', '__setattr__', 'remove_column', 'column', 'column_names', 'drop', '__hash__', 'itercolumns', '__doc__', 'to_pandas', 'cast', '__subclasshook__', 'rename_columns', 'num_rows', 'equals', 'nbytes', '__dir__', '__init__', 'to_string', '__init_subclass__', 'add_column', '__class__', 'set_column', '__str__', 'num_columns', '__sizeof__', 'combine_chunks', '__eq__', 'filter', '__setstate__', 'append_column', 'schema', '__gt__', '__getitem__', '__reduce_ex__', 'flatten', '__reduce__', '__lt__', 'columns']
	def to_pydict(self):
		pass

	def slice(self, offset=0, length=None):
		pass

	def replace_schema_metadata(self, metadata=None):
		pass

	def to_batches(self, max_chunksize=None):
		pass

	def validate(self, *, full=False):
		pass

	def field(self, i):
		pass

	def remove_column(self, int i):
		pass

	def column(self, i):
		pass

	def drop(self, columns):
		pass

	def itercolumns(self):
		pass

	def cast(self, Schema target_schema, bool safe=True):
		pass

	def rename_columns(self, names):
		pass

	def equals(self, Table other, bool check_metadata=False):
		pass

	def to_string(self, *, show_metadata=False, preview_cols=0):
		pass

	def add_column(self, int i, field_, column):
		pass

	def set_column(self, int i, field_, column):
		pass

	def __sizeof__(self):
		pass

	def combine_chunks(self, MemoryPool memory_pool=None):
		pass

	def filter(self, mask, null_selection_behavior=u'drop'):
		pass

	def append_column(self, field_, column):
		pass

	def flatten(self, MemoryPool memory_pool=None):
		pass

	def __reduce__(self):
		pass

