import pyarrow.lib
from datasets.table import MemoryMappedTable

x = list(dir(pyarrow.lib.Table))
y = list(dir(MemoryMappedTable))

class_type = pyarrow.lib.Table
name = class_type.__name__
basename = 'pyarrow.lib.Table'

intersect = list(set(x).intersection(y))
print(intersect)

for item in intersect:
	eval_string = f'{basename}.{item}.__doc__'
	doc = eval(eval_string)
	try:
		first_line = doc.split('\n')[0]
		if (name not in first_line):
			continue
		signature = first_line.split('.')[1]
		print(f'\tdef {signature}:')
		print('\t\tpass')
		print('')
	except:
		continue
