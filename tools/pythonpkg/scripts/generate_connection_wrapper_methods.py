import os
import json

os.chdir(os.path.dirname(__file__))

JSON_PATH = os.path.join("connection_methods.json")
WRAPPER_JSON_PATH = os.path.join("connection_wrapper_methods.json")
DUCKDB_INIT_FILE = os.path.join("..", "duckdb", "__init__.py")

START_MARKER = "# START OF CONNECTION WRAPPER"
END_MARKER = "# END OF CONNECTION WRAPPER"


def generate():
    # Read the DUCKDB_INIT_FILE file
    with open(DUCKDB_INIT_FILE, 'r') as source_file:
        source_code = source_file.readlines()

    start_index = -1
    end_index = -1
    for i, line in enumerate(source_code):
        if line.startswith(START_MARKER):
            if start_index != -1:
                raise ValueError("Encountered the START_MARKER a second time, quitting!")
            start_index = i
        elif line.startswith(END_MARKER):
            if end_index != -1:
                raise ValueError("Encountered the END_MARKER a second time, quitting!")
            end_index = i

    if start_index == -1 or end_index == -1:
        raise ValueError("Couldn't find start or end marker in source file")

    start_section = source_code[: start_index + 1]
    end_section = source_code[end_index:]
    # ---- Generate the definition code from the json ----

    methods = []

    # Read the JSON file
    with open(JSON_PATH, 'r') as json_file:
        connection_methods = json.load(json_file)

    with open(WRAPPER_JSON_PATH, 'r') as json_file:
        wrapper_methods = json.load(json_file)

    methods.extend(connection_methods)
    methods.extend(wrapper_methods)

    # On DuckDBPyConnection these are read_only_properties, they're basically functions without requiring () to invoke
    # that's not possible on 'duckdb' so it becomes a function call with no arguments (i.e duckdb.description())
    READONLY_PROPERTY_NAMES = ['description', 'rowcount']

    # These methods are not directly DuckDBPyConnection methods,
    # they first call 'from_df' and then call a method on the created DuckDBPyRelation
    SPECIAL_METHOD_NAMES = [x['name'] for x in wrapper_methods if x['name'] not in READONLY_PROPERTY_NAMES]

    def generate_arguments(name, method) -> str:
        arguments = []
        if name in SPECIAL_METHOD_NAMES:
            # We add 'df' to these methods because they operate on a DataFrame
            arguments.append('df')

        if 'args' in method:
            for arg in method['args']:
                res = arg['name']
                if 'default' in arg:
                    res += f" = {arg['default']}"
                arguments.append(res)
        arguments.append('**kwargs')
        return ', '.join(arguments)

    def generate_parameters(name, method) -> str:
        if name in READONLY_PROPERTY_NAMES:
            return ''
        arguments = []
        if 'args' in method:
            for arg in method['args']:
                arguments.append(f"{arg['name']}")
        arguments.append('**kwargs')
        result = ', '.join(arguments)
        return '(' + result + ')'

    def generate_duck_call(name) -> str:
        duck_call = ''
        if name in SPECIAL_METHOD_NAMES:
            duck_call += 'from_df(df).'

        REMAPPED_FUNCTIONS = {'alias': 'set_alias', 'query_df': 'query'}
        if name in REMAPPED_FUNCTIONS:
            function_name = REMAPPED_FUNCTIONS[name]
        else:
            function_name = name
        duck_call += function_name
        return duck_call

    def generate_function_call(method, duck_call, parameters) -> str:
        lines = []
        lines.append('previous_frame = inspect.currentframe().f_back')
        lines.append('globals = previous_frame.f_globals')
        lines.append('locals = previous_frame.f_locals.copy()')

        arguments = []
        if 'args' in method:
            for arg in method['args']:
                res = arg['name']
                arguments.append(res.replace('*', ''))
        arguments.append('conn')
        arguments.append('kwargs')

        for arg in arguments:
            lines.append(f"locals['{arg}'] = {arg}")
        lines.append(f"exec('result = conn.{duck_call}{parameters}', globals, locals)")
        lines.append("return locals['result']")
        return '\n'.join([f'    {x}' for x in lines])

    def create_definition(name, method) -> str:
        arguments = generate_arguments(name, method)
        parameters = generate_parameters(name, method)
        duck_call = generate_duck_call(name)
        function_call = generate_function_call(method, duck_call, parameters)

        func = f"""
def {name}({arguments}):
    if 'connection' in kwargs:
        conn = kwargs.pop('connection')
    else:
        conn = duckdb.connect(":default:")
{function_call}
_exported_symbols.append('{name}')
"""
        return func

    # We have "duplicate" methods, which are overloaded
    written_methods = set()

    body = []
    for method in methods:
        if isinstance(method['name'], list):
            names = method['name']
        else:
            names = [method['name']]

        # Artificially add 'connection' keyword argument
        if 'kwargs' not in method:
            method['kwargs'] = []
        method['kwargs'].append({'name': 'connection', 'type': 'DuckDBPyConnection'})

        for name in names:
            if name in written_methods:
                continue
            if name in ['arrow', 'df']:
                # These methods are ambiguous and are handled in C++ code instead
                continue
            body.append(create_definition(name, method))
            written_methods.add(name)

    # ---- End of generation code ----

    with_newlines = body
    # Recreate the file content by concatenating all the pieces together

    new_content = start_section + with_newlines + end_section

    # Write out the modified DUCKDB_INIT_FILE file
    with open(DUCKDB_INIT_FILE, 'w') as source_file:
        source_file.write("".join(new_content))


if __name__ == '__main__':
    raise ValueError("Please use 'generate_connection_code.py' instead of running the individual script(s)")
    # generate()
