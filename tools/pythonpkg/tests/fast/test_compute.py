import pytest
pa = pytest.importorskip("pyarrow")
pc = pytest.importorskip("pyarrow.compute")

#import duckdb.compute

class TestCompute(object):
	def test_sort_indices(self):
		# Create an example array
		data = [5, 2, 8, 1, 6]
		array = pa.array(data)

		print(array.__class__)
		# Compute indices that would sort the array
		sorted_indices = pc.sort_indices(array)

		# Sort the original array using the sorted indices
		sorted_array = pc.take(array, sorted_indices)

		print("Original array:", data)
		print("Sorted array:", sorted_array.to_pylist())
	
