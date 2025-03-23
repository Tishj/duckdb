# this script creates a single header + source file combination out of the DuckDB sources
import os
import re
import sys
import shutil
import subprocess
from python_helpers import open_utf8, normalize_path, get_git_describe, git_dev_version
from typing import List, Optional

### File Gathering ###


def third_party_includes():
    includes = []
    includes += [os.path.join('third_party', 'concurrentqueue')]
    includes += [os.path.join('third_party', 'fast_float')]
    includes += [os.path.join('third_party', 'fastpforlib')]
    includes += [os.path.join('third_party', 'fmt', 'include')]
    includes += [os.path.join('third_party', 'fsst')]
    includes += [os.path.join('third_party', 'httplib')]
    includes += [os.path.join('third_party', 'hyperloglog')]
    includes += [os.path.join('third_party', 'jaro_winkler')]
    includes += [os.path.join('third_party', 'jaro_winkler', 'details')]
    includes += [os.path.join('third_party', 'libpg_query')]
    includes += [os.path.join('third_party', 'libpg_query', 'include')]
    includes += [os.path.join('third_party', 'lz4')]
    includes += [os.path.join('third_party', 'brotli', 'include')]
    includes += [os.path.join('third_party', 'brotli', 'common')]
    includes += [os.path.join('third_party', 'brotli', 'dec')]
    includes += [os.path.join('third_party', 'brotli', 'enc')]
    includes += [os.path.join('third_party', 'mbedtls', 'include')]
    includes += [os.path.join('third_party', 'mbedtls', 'library')]
    includes += [os.path.join('third_party', 'miniz')]
    includes += [os.path.join('third_party', 'pcg')]
    includes += [os.path.join('third_party', 're2')]
    includes += [os.path.join('third_party', 'skiplist')]
    includes += [os.path.join('third_party', 'tdigest')]
    includes += [os.path.join('third_party', 'utf8proc')]
    includes += [os.path.join('third_party', 'utf8proc', 'include')]
    includes += [os.path.join('third_party', 'yyjson', 'include')]
    includes += [os.path.join('third_party', 'zstd', 'include')]
    return includes


def third_party_sources():
    sources = []
    sources += [os.path.join('third_party', 'fmt')]
    sources += [os.path.join('third_party', 'fsst')]
    sources += [os.path.join('third_party', 'miniz')]
    sources += [os.path.join('third_party', 're2')]
    sources += [os.path.join('third_party', 'hyperloglog')]
    sources += [os.path.join('third_party', 'skiplist')]
    sources += [os.path.join('third_party', 'fastpforlib')]
    sources += [os.path.join('third_party', 'utf8proc')]
    sources += [os.path.join('third_party', 'libpg_query')]
    sources += [os.path.join('third_party', 'mbedtls')]
    sources += [os.path.join('third_party', 'yyjson')]
    sources += [os.path.join('third_party', 'zstd')]
    return sources


# files always excluded
ALWAYS_EXCLUDED = normalize_path(
    [
        'src/amalgamation/duckdb.cpp',
        'src/amalgamation/duckdb.hpp',
        'src/amalgamation/parquet-amalgamation.cpp',
        'src/amalgamation/parquet-amalgamation.hpp',
    ]
)

### Constants ###

HEADER_FORMAT = """/*
{license}
*/

#pragma once
#define DUCKDB_AMALGAMATION 1
{define_extended}
#define DUCKDB_SOURCE_ID "{commit_hash}"
#define DUCKDB_VERSION "{version_string}"
#define DUCKDB_MAJOR_VERSION {major}
#define DUCKDB_MINOR_VERSION {minor}
#define DUCKDB_PATCH_VERSION "{patch}"
"""

### Utils ###


def copy_if_different(src, dest):
    if os.path.isfile(dest):
        # dest exists, check if the files are different
        with open_utf8(src, 'r') as f:
            source_text = f.read()
        with open_utf8(dest, 'r') as f:
            dest_text = f.read()
        if source_text == dest_text:
            # print("Skipping copy of " + src + ", identical copy already exists at " + dest)
            return
    # print("Copying " + src + " to " + dest)
    shutil.copyfile(src, dest)


def git_commit_hash():
    git_describe = get_git_describe()
    hash = git_describe.split('-')[2].lstrip('g')
    return hash


class IncludeStatement:
    def __init__(self, line_numbers: List[int], content: str, include_file: str):
        self.line_numbers = line_numbers
        self.content = content
        self.include_file = include_file
        self.full_path = include_file

    def __repr__(self):
        return self.include_file


def get_include(line_numbers, content) -> Optional[IncludeStatement]:
    if not content.startswith('#'):
        return None

    tokens = content.split(None, 2)
    if len(tokens) < 2:
        return None

    if tokens[0] == '#' and tokens[1] == 'include':
        path = tokens[2]
    elif tokens[0] == '#include':
        path = tokens[1]
    else:
        return None

    if path[0] != '"' or path[-1] != '"':
        return None

    return IncludeStatement(line_numbers, content, path[1:-1])


def collect_includes(text) -> List[IncludeStatement]:
    include_statements = []
    lines = text.splitlines()

    i = 0
    while i < len(lines):
        line = lines[i].strip()

        line_numbers = [i]
        # Handle potential line continuations
        while line.endswith('\\') and i + 1 < len(lines):
            i += 1
            line = line[:-1] + lines[i].strip()
            line_numbers.append(i)
            continue

        include = get_include(line_numbers, line)
        if include:
            include_statements.append(include)
        i += 1
    return include_statements


### Classes ###


class FileState:
    def __init__(self, filepath):
        self.path = filepath
        self.content: str = ''
        self.includes: List[IncludeStatement] = []

    def __repr__(self):
        return self.path

    def get_content(self):
        # first read this file
        with open_utf8(self.path, 'r') as f:
            self.content = f.read()

    def cleanup_file(self):
        # remove all "#pragma once" notifications
        self.content = re.sub('#pragma once', '', self.content)

    def add_license(self, license_index: int):
        self.content = (
            "\n\n// LICENSE_CHANGE_BEGIN\n// The following code up to LICENSE_CHANGE_END is subject to THIRD PARTY LICENSE #%s\n// See the end of this file for a list\n\n"
            % str(license_index + 1)
            + self.content
            + "\n\n// LICENSE_CHANGE_END\n"
        )

    def get_includes(self, skip_duckdb_includes: bool, include_paths: List[str]):
        # find all the includes referred to in the directory
        include_statements = collect_includes(self.content)
        # figure out where they are located
        for include_statement in include_statements:
            included_file = include_statement.include_file
            if skip_duckdb_includes and 'duckdb' in included_file:
                continue
            if (
                'extension_helper.cpp' in self.path
                and (included_file.endswith('_extension.hpp'))
                or included_file == 'generated_extension_loader.hpp'
                or included_file == 'generated_extension_headers.hpp'
            ):
                continue
            if 'allocator.cpp' in self.path and included_file.endswith('jemalloc_extension.hpp'):
                continue
            if included_file in self.includes:
                raise Exception(f"duplicate include {included_file} in file {self.path}")
            included_file = os.sep.join(included_file.split('/'))
            found = False
            for include_path in include_paths:
                ipath = os.path.join(include_path, included_file)
                if os.path.isfile(ipath):
                    include_statement.full_path = ipath
                    self.includes.append(include_statement)
                    found = True
                    break
            if not found:
                raise Exception(f'Could not find include file "{included_file}", included from file "{self.path}"')


class Amalgamator:
    def __init__(
        self,
        *,
        source_file: Optional[str] = None,
        header_file: Optional[str] = None,
        extended: Optional[bool] = None,
        linenumbers: Optional[bool] = None,
        splits: Optional[str] = None,
    ):
        self.output_directory = os.path.join('src', 'amalgamation')
        self.temp_header = 'duckdb.hpp.tmp'
        self.temp_source = 'duckdb.cpp.tmp'

        if header_file:
            self.header_file = header_file
        else:
            self.header_file = os.path.join(self.output_directory, "duckdb.hpp")

        if source_file:
            self.source_file = source_file
        else:
            self.source_file = os.path.join(self.output_directory, "duckdb.cpp")

        if extended != None:
            self.extended_amalgamation = extended
        else:
            self.extended_amalgamation = False

        if linenumbers != None:
            self.linenumbers = linenumbers
        else:
            self.linenumbers = False

        self.splits = splits
        self.skip_duckdb_includes = False
        self.src_dir = 'src'
        self.include_dir = os.path.join('src', 'include')

        # Map from filepath -> FileState
        self.discovered_files: Dict[str, FileState] = {}
        self.written_files = set()
        self.licenses = []

        # files included in the amalgamated "duckdb.hpp" file
        self.main_header_files: List[str] = []
        # include paths for where to search for include files during amalgamation
        self.include_paths: List[str] = []
        # paths of where to look for files to compile and include to the final amalgamation
        self.compile_directories: List[str] = []
        # files excluded from the amalgamation
        self.excluded_files: List[str] = []
        # files excluded from individual file compilation during test_compile
        self.excluded_compilation_files: List[str] = []

    def get_header_files(self):
        res = [
            os.path.join(self.include_dir, 'duckdb.hpp'),
            os.path.join(self.include_dir, 'duckdb.h'),
            os.path.join(self.include_dir, 'duckdb', 'common', 'types', 'date.hpp'),
            os.path.join(self.include_dir, 'duckdb', 'common', 'adbc', 'adbc.h'),
            os.path.join(self.include_dir, 'duckdb', 'common', 'adbc', 'adbc.hpp'),
            os.path.join(self.include_dir, 'duckdb', 'common', 'arrow', 'arrow.hpp'),
            os.path.join(self.include_dir, 'duckdb', 'common', 'arrow', 'arrow_converter.hpp'),
            os.path.join(self.include_dir, 'duckdb', 'common', 'arrow', 'arrow_wrapper.hpp'),
            os.path.join(self.include_dir, 'duckdb', 'common', 'types', 'blob.hpp'),
            os.path.join(self.include_dir, 'duckdb', 'common', 'types', 'decimal.hpp'),
            os.path.join(self.include_dir, 'duckdb', 'common', 'types', 'hugeint.hpp'),
            os.path.join(self.include_dir, 'duckdb', 'common', 'types', 'uhugeint.hpp'),
            os.path.join(self.include_dir, 'duckdb', 'common', 'types', 'uuid.hpp'),
            os.path.join(self.include_dir, 'duckdb', 'common', 'types', 'interval.hpp'),
            os.path.join(self.include_dir, 'duckdb', 'common', 'types', 'timestamp.hpp'),
            os.path.join(self.include_dir, 'duckdb', 'common', 'types', 'time.hpp'),
            os.path.join(self.include_dir, 'duckdb', 'common', 'serializer', 'buffered_file_writer.hpp'),
            os.path.join(self.include_dir, 'duckdb', 'common', 'serializer', 'memory_stream.hpp'),
            os.path.join(self.include_dir, 'duckdb', 'main', 'appender.hpp'),
            os.path.join(self.include_dir, 'duckdb', 'main', 'client_context.hpp'),
            os.path.join(self.include_dir, 'duckdb', 'main', 'extension_util.hpp'),
            os.path.join(self.include_dir, 'duckdb', 'function', 'function.hpp'),
            os.path.join(self.include_dir, 'duckdb', 'function', 'table_function.hpp'),
            os.path.join(self.include_dir, 'duckdb', 'parser', 'parsed_data', 'create_table_function_info.hpp'),
            os.path.join(self.include_dir, 'duckdb', 'parser', 'parsed_data', 'create_copy_function_info.hpp'),
        ]
        if not self.extended_amalgamation:
            return res

        def add_include_dir(dirpath):
            return [os.path.join(dirpath, x) for x in os.listdir(dirpath)]

        res += [
            os.path.join(self.include_dir, x)
            for x in [
                'duckdb/planner/expression/bound_constant_expression.hpp',
                'duckdb/planner/expression/bound_function_expression.hpp',
                'duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp',
                'duckdb/parser/parsed_data/create_table_info.hpp',
                'duckdb/planner/parsed_data/bound_create_table_info.hpp',
                'duckdb/parser/constraints/not_null_constraint.hpp',
                'duckdb/storage/data_table.hpp',
                'duckdb/function/pragma_function.hpp',
                'duckdb/parser/qualified_name.hpp',
                'duckdb/parser/parser.hpp',
                'duckdb/planner/binder.hpp',
                'duckdb/storage/object_cache.hpp',
                'duckdb/planner/table_filter.hpp',
                "duckdb/storage/statistics/base_statistics.hpp",
                "duckdb/planner/filter/conjunction_filter.hpp",
                "duckdb/planner/filter/constant_filter.hpp",
                "duckdb/common/types/vector_cache.hpp",
                "duckdb/common/string_map_set.hpp",
                "duckdb/planner/filter/null_filter.hpp",
                "duckdb/common/arrow/arrow_wrapper.hpp",
                "duckdb/common/hive_partitioning.hpp",
                "duckdb/common/union_by_name.hpp",
                "duckdb/planner/operator/logical_get.hpp",
                "duckdb/common/compressed_file_system.hpp",
            ]
        ]
        res += add_include_dir(os.path.join(self.include_dir, 'duckdb/parser/expression'))
        res += add_include_dir(os.path.join(self.include_dir, 'duckdb/parser/parsed_data'))
        res += add_include_dir(os.path.join(self.include_dir, 'duckdb/parser/tableref'))
        res = normalize_path(res)
        return res

    def initialize(self):
        self.main_header_files = self.get_header_files()
        self.include_paths = [self.include_dir] + third_party_includes()
        self.compile_directories = [self.src_dir] + third_party_sources()
        self.excluded_files = ['grammar.cpp', 'grammar.hpp', 'symbols.cpp']
        self.excluded_compilation_files = self.excluded_files + ['gram.hpp', 'kwlist.hpp', "duckdb-c.cpp"]

    def list_include_dirs(self):
        return self.include_paths

    def need_to_write_file(self, file_path, ignore_excluded=False):
        if ignore_excluded:
            excluded_paths = []
        else:
            excluded_paths = self.excluded_files

        if self.output_directory in file_path:
            return False
        if file_path in ALWAYS_EXCLUDED:
            return False
        if file_path.split(os.sep)[-1] in excluded_paths:
            # file is in ignored files set
            return False
        return True

    def find_or_add_license(self, original_file) -> int:
        file = original_file
        license = ""
        while True:
            (file, _) = os.path.split(file)
            if file == "":
                break
            potential_license = os.path.join(file, "LICENSE")
            if os.path.exists(potential_license):
                license = potential_license
        if license == "":
            raise "Could not find license for %s" % original_file

        if license not in licenses:
            licenses += [license]
        return licenses.index(license)

    def get_file_state(self, filename: str) -> Optional[FileState]:
        if not self.need_to_write_file(filename):
            return None

        state = FileState(filename)
        state.get_content()
        if filename.startswith("third_party") and not filename.endswith("LICENSE"):
            license_index = self.find_or_add_license(filename)
            state.add_license(license_index)
        state.get_includes(self.skip_duckdb_includes, self.include_paths)
        return state

    def gather_file(self, filepath: str, result: List[str]):
        if filepath in self.discovered_files:
            # If this FileState is already prepared, directly add it
            result.append(filepath)
            return

        # Check if this file needs to be skipped entirely
        file_state = self.get_file_state(filepath)
        if not file_state:
            return

        # find the linenr of the final #include statement we parsed
        if len(file_state.includes) > 0:
            index = file_state.content.find(file_state.includes[-1])
            linenr = len(file_state.content[:index].split('\n'))

            # now write all the dependencies of this header first
            for i, include_file in enumerate(file_state.include_files):
                self.gather_file(include_file, result)
                if self.linenumbers and i == len(file_state.include_files) - 1:
                    # for the last include statement, we also include a #line directive
                    include_text += '\n#line %d "%s"\n' % (linenr, filepath)
                file_state.content = file_state.content.replace(file_state.includes[i], include_text)
        self.discovered_files[filepath] = file_state

        # add the initial line here
        if self.linenumbers:
            file_state.content = '\n#line 1 "%s"\n' % (filename,) + file_state.content
        file_state.cleanup_file()
        result.append(filepath)

    def gather_directory(self, directory_path: str, result: List[str]) -> str:
        files = os.listdir(directory_path)
        files.sort()
        directory_contents: Dict[str, List[str]] = {}
        for fname in files:
            if fname in excluded_files:
                continue
            fpath = os.path.join(directory_path, fname)
            if os.path.isdir(fpath):
                self.gather_directory(fpath, result)
            elif fname.endswith('.cpp') or fname.endswith('.c') or fname.endswith('.cc'):
                self.gather_file(fpath, result)

    def write_files(self, files: List[FileState]) -> str:
        content = ''
        for file in files:
            if file in self.written_files:
                continue
            if file not in self.discovered_files:
                print(f"File ({file}) could not be found in the list of discovered files!")
                print("List of discovered files:")
                for discovered_file in self.discovered_files:
                    print(f'\t{discovered_file}')
                exit(1)
            file_state = self.discovered_files.get(file)
            content += file_state.content
            self.written_files.add(file)
        return content

    def generate_header(self):
        print(
            """
-----------------------
-- Writing {self.header_file} --
-----------------------
"""
        )
        with open_utf8(self.temp_header, 'w+') as hfile:
            license = []
            self.gather_file("LICENSE", license)
            define_extended = "#define DUCKDB_AMALGAMATION_EXTENDED 1" if self.extended_amalgamation else ''
            commit_hash = git_commit_hash()
            version_string = git_dev_version()
            major, minor, patch = version_string.lstrip('v').split('.')

            hfile.write(
                HEADER_FORMAT.format(
                    license=self.write_files(license),
                    define_extended=define_extended,
                    commit_hash=commit_hash,
                    version_string=version_string,
                    major=major,
                    minor=minor,
                    patch=patch,
                )
            )

            header_files = []
            for header_file in self.main_header_files:
                self.gather_file(header_file, header_files)

            for file_path in header_files:
                content = self.write_file()
                if content:
                    hfile.write(content)

    def generate(self):
        # Create output directory
        if os.path.exists(self.output_directory):
            shutil.rmtree(self.output_directory)
        os.makedirs(self.output_directory)

        self.generate_header()
        if self.splits > 1:
            self.generate_with_splits()
        else:
            self.generate_single_file()


def generate_amalgamation(source_file, header_file):
    # now construct duckdb.cpp
    print("------------------------")
    print("-- Writing " + source_file + " --")
    print("------------------------")

    # scan all the .cpp files
    with open_utf8(temp_source, 'w+') as sfile:
        header_file_name = header_file.split(os.sep)[-1]
        sfile.write('#include "' + header_file_name + '"\n\n')
        sfile.write("#ifndef DUCKDB_AMALGAMATION\n#error header mismatch\n#endif\n\n")
        sfile.write("#if (!defined(DEBUG) && !defined NDEBUG)\n#define NDEBUG\n#endif\n\n")
        for compile_dir in compile_directories:
            sfile.write(write_dir(compile_dir))

        sfile.write('\n\n/*\n')
        license_idx = 0
        for license in licenses:
            sfile.write("\n\n\n### THIRD PARTY LICENSE #%s ###\n\n" % str(license_idx + 1))
            sfile.write(write_file(license))
            license_idx += 1
        sfile.write('\n\n*/\n')

    copy_if_different(temp_header, header_file)
    copy_if_different(temp_source, source_file)
    try:
        os.remove(temp_header)
        os.remove(temp_source)
    except:
        pass


def list_files(dname, file_list):
    files = os.listdir(dname)
    files.sort()
    for fname in files:
        if fname in excluded_files:
            continue
        fpath = os.path.join(dname, fname)
        if os.path.isdir(fpath):
            list_files(fpath, file_list)
        elif fname.endswith(('.cpp', '.c', '.cc')):
            if need_to_write_file(fpath):
                file_list.append(fpath)


def list_sources():
    file_list = []
    for compile_dir in compile_directories:
        list_files(compile_dir, file_list)
    return file_list


def list_include_files_recursive(dname, file_list):
    files = os.listdir(dname)
    files.sort()
    for fname in files:
        if fname in excluded_files:
            continue
        fpath = os.path.join(dname, fname)
        if os.path.isdir(fpath):
            list_include_files_recursive(fpath, file_list)
        elif fname.endswith(('.hpp', '.ipp', '.h', '.hh', '.tcc', '.inc')):
            file_list.append(fpath)


def list_includes_files(include_dirs):
    file_list = []
    for include_dir in include_dirs:
        list_include_files_recursive(include_dir, file_list)
    return file_list


def list_includes():
    return list_includes_files(include_paths)


def gather_files(dir, source_files, header_files):
    files = os.listdir(dir)
    files.sort()
    for fname in files:
        if fname in excluded_files:
            continue
        fpath = os.path.join(dir, fname)
        if os.path.isdir(fpath):
            gather_files(fpath, source_files, header_files)
        else:
            gather_file(fpath, source_files, header_files)


def write_license(hfile):
    hfile.write("// See https://raw.githubusercontent.com/duckdb/duckdb/main/LICENSE for licensing information\n\n")


def generate_amalgamation_splits(source_file, header_file, nsplits):
    # gather all files to read and write
    source_files = []
    header_files = []
    for compile_dir in compile_directories:
        if compile_dir != src_dir:
            continue
        gather_files(compile_dir, source_files, header_files)

    # write duckdb-internal.hpp
    if '.hpp' in header_file:
        internal_header_file = header_file.replace('.hpp', '-internal.hpp')
    elif '.h' in header_file:
        internal_header_file = header_file.replace('.h', '-internal.h')
    else:
        raise "Unknown extension of header file"

    temp_internal_header = internal_header_file + '.tmp'

    with open_utf8(temp_internal_header, 'w+') as f:
        write_license(f)
        for hfile in header_files:
            f.write(hfile)

    # count the total amount of bytes in the source files
    total_bytes = 0
    for sfile in source_files:
        total_bytes += len(sfile)

    # now write the individual splits
    # we approximate the splitting up by making every file have roughly the same amount of bytes
    split_bytes = total_bytes / nsplits
    current_bytes = 0
    partitions = []
    partition_names = []
    current_partition = []
    current_partition_idx = 1
    for sfile in source_files:
        current_partition.append(sfile)
        current_bytes += len(sfile)
        if current_bytes >= split_bytes:
            partition_names.append(str(current_partition_idx))
            partitions.append(current_partition)
            current_partition = []
            current_bytes = 0
            current_partition_idx += 1
    if len(current_partition) > 0:
        partition_names.append(str(current_partition_idx))
        partitions.append(current_partition)
        current_partition = []
        current_bytes = 0

    # generate partitions from the third party libraries
    for compile_dir in compile_directories:
        if compile_dir != src_dir:
            partition_names.append(compile_dir.split(os.sep)[-1])
            partitions.append(write_dir(compile_dir))

    header_file_name = header_file.split(os.sep)[-1]
    internal_header_file_name = internal_header_file.split(os.sep)[-1]

    partition_fnames: List[Tuple[str, str]] = []
    for i, partition in enumerate(partitions):
        partition_name = source_file.replace('.cpp', f'-{partition_names[i]}.cpp')
        temp_partition_name = partition_name + '.tmp'
        partition_fnames.append((partition_name, temp_partition_name))
        with open_utf8(temp_partition_name, 'w+') as f:
            write_license(f)
            f.write('#include "%s"\n#include "%s"' % (header_file_name, internal_header_file_name))
            f.write(
                '''
#ifndef DUCKDB_AMALGAMATION
#error header mismatch
#endif
'''
            )
            for sfile in partition:
                f.write(sfile)

    copy_if_different(temp_header, header_file)
    copy_if_different(temp_internal_header, internal_header_file)
    try:
        os.remove(temp_header)
        os.remove(temp_internal_header)
    except:
        pass
    for partition in partition_fnames:
        copy_if_different(p[1], p[0])
        try:
            os.remove(p[1])
        except:
            pass


def configure_parser():
    import argparse

    parser = argparse.ArgumentParser(
        description='Create a single header + source file combination out of the DuckDB sources'
    )
    parser.add_argument(
        '--extended',
        action='store_true',
        help='Generate extended amalgamation with additional header files',
        default=False,
    )
    parser.add_argument(
        '--linenumbers', action='store_true', help='Include line numbers in the amalgamation', default=False
    )
    parser.add_argument(
        '--no-linenumbers', action='store_false', dest='linenumbers', help='Exclude line numbers from the amalgamation'
    )
    parser.add_argument('--header', help='Specify header file path', default=None)
    parser.add_argument('--source', help='Specify source file path', default=None)
    parser.add_argument('--splits', type=int, help='Number of splits for the amalgamation', default=1)
    parser.add_argument('--list-sources', action='store_true', help='List all source files')
    parser.add_argument('--list-objects', action='store_true', help='List all object files')
    parser.add_argument('--includes', action='store_true', help='List include directories with -I prefix')
    parser.add_argument('--include-directories', action='store_true', help='List include directories')
    return parser


def main():
    parser = configure_parser()
    args = parser.parse_args()

    header_file = None
    source_file = None
    extended = None
    linenumbers = None

    if args.header:
        header_file = os.path.join(*args.header.split('/'))
    if args.source:
        source_file = os.path.join(*args.source.split('/'))
    extended = args.extended
    linenumbers = args.linenumbers

    amalgamator = Amalgamator(
        header_file=header_file, source_file=source_file, extended=extended, linenumbers=linenumbers, splits=args.splits
    )
    amalgamator.initialize()

    # Handle listing commands
    if args.list_sources:
        file_list = list_sources()
        print('\n'.join(file_list))
        sys.exit(1)

    if args.list_objects:
        file_list = list_sources()
        print(' '.join([x.rsplit('.', 1)[0] + '.o' for x in file_list]))
        sys.exit(1)

    if args.includes:
        include_dirs = amalgamator.list_include_dirs()
        print(' '.join(['-I' + x for x in include_dirs]))
        sys.exit(1)

    if args.include_directories:
        include_dirs = amalgamator.list_include_dirs()
        print('\n'.join(include_dirs))
        sys.exit(1)

    # Generate amalgamation
    amalgamator.generate()


if __name__ == "__main__":
    main()
