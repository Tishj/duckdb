import ast
import os
from glob import iglob
import pathlib


class ImportCheckerResult:
    def __init__(self):
        self.errors = []
        self.warnings = []

    def add_warning(self, name):
        # This should be used to detect `try: import optional_module except: return` anti-patterns
        # These should be replaced with pytest.importorskip
        self.warnings.append(name)

    def add_error(self, name):
        self.errors.append(name)


def check_unsafe_imports(file_path, optional_modules):
    with open(file_path, 'r') as f:
        tree = ast.parse(f.read(), filename=file_path)

    result = ImportCheckerResult()

    # Helper function to traverse the AST recursively
    def traverse(node):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name in optional_modules:
                    result.add_error(alias.name)
        elif isinstance(node, ast.ImportFrom):
            if node.module in optional_modules:
                print(node.module)
                result.add_error(node.module)
        elif isinstance(node, ast.FunctionDef):
            for stmt in node.body:
                traverse(stmt)
        elif isinstance(node, ast.ClassDef):
            for stmt in node.body:
                traverse(stmt)
            for method in node.body:
                traverse(method)

    for node in tree.body:
        traverse(node)

    return result


def process_directory(directory_path, optional_modules):
    unguarded_imports_dict = {}
    file_list = [
        (f, pathlib.Path(f).name)
        for f in iglob(directory_path + '/**/*', recursive=True)
        if os.path.isfile(f) and f.endswith('.py')
    ]
    for file_path, file_name in file_list:
        result = check_unsafe_imports(file_path, optional_modules)
        unguarded_imports = result.errors
        if unguarded_imports:
            unguarded_imports_dict[file_name] = unguarded_imports
    return unguarded_imports_dict


def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    requirements_path = os.path.join(script_dir, '..', 'tests')
    optional_modules = ["pyarrow", "torch", "polars", "adbc_driver_manager", "tensorflow"]
    errors = process_directory(requirements_path, optional_modules)
    if len(errors) == 0:
        return
    print("Unguarded imports detected!")
    print(
        "We use certain modules in tests that are not always available on every OS, for this reason we use pytest.importorskip('<module>') instead"
    )
    print("The following tests have improperly guarded imports")
    for key, value in errors.items():
        print(f"{key}:")
        for module in value:
            print(f"\t{module}")
    exit(1)


if __name__ == '__main__':
    main()
