import pytest
pa = pytest.importorskip("pyarrow")
import duckdb
import duckdb.duckdb.compute as pc

class TestCompute(object):
	def test_sort_indices(self):
		arr = pa.array([1, 2, None, 0])
		result = pc.sort_indices(arr)
		assert result.to_pylist() == [3, 0, 1, 2]
		result = pc.sort_indices(arr, sort_keys=[("dummy", "ascending")])
		assert result.to_pylist() == [3, 0, 1, 2]
		result = pc.sort_indices(arr, sort_keys=[("dummy", "descending")])
		assert result.to_pylist() == [1, 0, 3, 2]
		result = pc.sort_indices(arr, sort_keys=[("dummy", "descending")], null_placement="at_start")
		assert result.to_pylist() == [2, 1, 0, 3]
		# Positional `sort_keys`
		result = pc.sort_indices(arr, [("dummy", "descending")], null_placement="at_start")
		assert result.to_pylist() == [2, 1, 0, 3]
		## Using SortOptions
		#result = pc.sort_indices(
		#	arr, options=pc.SortOptions(sort_keys=[("dummy", "descending")])
		#)
		#assert result.to_pylist() == [1, 0, 3, 2]
		#result = pc.sort_indices(
		#	arr, options=pc.SortOptions(sort_keys=[("dummy", "descending")], null_placement="at_start")
		#)
		#assert result.to_pylist() == [2, 1, 0, 3]
