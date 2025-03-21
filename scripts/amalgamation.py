# this script creates a single header + source file combination out of the DuckDB sources
import os
import re
import sys
import shutil
import subprocess
from python_helpers import open_utf8, normalize_path, get_git_describe, git_dev_version
from typing import List, Optional

### File Gathering ###

def get_header_files(extended: bool = False):
    res = [
        os.path.join(include_dir, 'duckdb.hpp'),
        os.path.join(include_dir, 'duckdb.h'),
        os.path.join(include_dir, 'duckdb', 'common', 'types', 'date.hpp'),
        os.path.join(include_dir, 'duckdb', 'common', 'adbc', 'adbc.h'),
        os.path.join(include_dir, 'duckdb', 'common', 'adbc', 'adbc.hpp'),
        os.path.join(include_dir, 'duckdb', 'common', 'arrow', 'arrow.hpp'),
        os.path.join(include_dir, 'duckdb', 'common', 'arrow', 'arrow_converter.hpp'),
        os.path.join(include_dir, 'duckdb', 'common', 'arrow', 'arrow_wrapper.hpp'),
        os.path.join(include_dir, 'duckdb', 'common', 'types', 'blob.hpp'),
        os.path.join(include_dir, 'duckdb', 'common', 'types', 'decimal.hpp'),
        os.path.join(include_dir, 'duckdb', 'common', 'types', 'hugeint.hpp'),
        os.path.join(include_dir, 'duckdb', 'common', 'types', 'uhugeint.hpp'),
        os.path.join(include_dir, 'duckdb', 'common', 'types', 'uuid.hpp'),
        os.path.join(include_dir, 'duckdb', 'common', 'types', 'interval.hpp'),
        os.path.join(include_dir, 'duckdb', 'common', 'types', 'timestamp.hpp'),
        os.path.join(include_dir, 'duckdb', 'common', 'types', 'time.hpp'),
        os.path.join(include_dir, 'duckdb', 'common', 'serializer', 'buffered_file_writer.hpp'),
        os.path.join(include_dir, 'duckdb', 'common', 'serializer', 'memory_stream.hpp'),
        os.path.join(include_dir, 'duckdb', 'main', 'appender.hpp'),
        os.path.join(include_dir, 'duckdb', 'main', 'client_context.hpp'),
        os.path.join(include_dir, 'duckdb', 'main', 'extension_util.hpp'),
        os.path.join(include_dir, 'duckdb', 'function', 'function.hpp'),
        os.path.join(include_dir, 'duckdb', 'function', 'table_function.hpp'),
        os.path.join(include_dir, 'duckdb', 'parser', 'parsed_data', 'create_table_function_info.hpp'),
        os.path.join(include_dir, 'duckdb', 'parser', 'parsed_data', 'create_copy_function_info.hpp'),
    ]
    if not extended:
        return res

    def add_include_dir(dirpath):
        return [os.path.join(dirpath, x) for x in os.listdir(dirpath)]

    res += [
        os.path.join(include_dir, x)
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
    res += add_include_dir(os.path.join(include_dir, 'duckdb/parser/expression'))
    res += add_include_dir(os.path.join(include_dir, 'duckdb/parser/parsed_data'))
    res += add_include_dir(os.path.join(include_dir, 'duckdb/parser/tableref'))
    res = normalize_path(res)
    return res

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

### Classes ###

class FileState:
    def __init__(self, filepath):
        self.path = filepath
        self.includes = []
        self.content: str = ''
        self.include_statements: List[str] = []
        self.include_files: List[str] = []

    def get_content(self):
        # first read this file
        with open_utf8(path, 'r') as f:
            self.content = f.read()

    def add_license(self, license_index: int):
        self.text = (
            "\n\n// LICENSE_CHANGE_BEGIN\n// The following code up to LICENSE_CHANGE_END is subject to THIRD PARTY LICENSE #%s\n// See the end of this file for a list\n\n"
            % str(license_index + 1)
            + self.text
            + "\n\n// LICENSE_CHANGE_END\n"
        )

    def get_includes(self):
        # find all the includes referred to in the directory
        regex_include_statements = re.findall("(^[\t ]*[#][\t ]*include[\t ]+[\"]([^\"]+)[\"])", self.text, flags=re.MULTILINE)
        # figure out where they are located
        for x in regex_include_statements:
            included_file = x[1]
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
            if x[0] in include_statements:
                raise Exception(f"duplicate include {x[0]} in file {self.path}")
            include_statements.append(x[0])
            included_file = os.sep.join(included_file.split('/'))
            found = False
            for include_path in include_paths:
                ipath = os.path.join(include_path, included_file)
                if os.path.isfile(ipath):
                    include_files.append(ipath)
                    found = True
                    break
            if not found:
                raise Exception('Could not find include file "' + included_file + '", included from file "' + self.path + '"')

    def cleanup_file(self):
        # remove all "#pragma once" notifications
        self.text = re.sub('#pragma once', '', self.text)
        return self.text

class Amalgamator:
    def __init__(self, *, source_file: Optional[str] = None, header_file: Optional[str] = None, extended: Optional[bool] = None, linenumbers: Optional[bool] = None):
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

        self.skip_duckdb_includes = False
        self.src_dir = 'src'
        self.include_dir = os.path.join('src', 'include')

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

    def initialize(self):
        self.main_header_files = get_header_files(self.extended_amalgamation)
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
            excluded_paths = self.excluded_paths

        if output_directory in state.path:
            return False
        if state.path in ALWAYS_EXCLUDED:
            return False
        if state.path.split(os.sep)[-1] in excluded_paths:
            # file is in ignored files set
            return False
        if state.path in written_files:
            # file is already written
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
        state.get_includes()
        return state

    # FIXME: change this to create a stack of FileState instead
    # followed by a method to write the FileStates 
    def write_file(self, filename: str) -> str:
        file_state = get_file_state(filename)
        if not file_state:
            return ''

        self.written_files.add(filename)

        # find the linenr of the final #include statement we parsed
        if len(file_state.include_statements) > 0:
            index = file_state.text.find(file_state.include_statements[-1])
            linenr = len(file_state.text[:index].split('\n'))

            # now write all the dependencies of this header first
            for i, include_file in enumerate(file_state.include_files):
                include_text = self.write_file(include_file)
                if self.linenumbers and i == len(file_state.include_files) - 1:
                    # for the last include statement, we also include a #line directive
                    include_text += '\n#line %d "%s"\n' % (linenr, filename)
                file_state.text = file_state.text.replace(file_state.include_statements[i], include_text)

        # add the initial line here
        if self.linenumbers:
            file_state.text = '\n#line 1 "%s"\n' % (filename,) + file_state.text
        # print(filename)
        # now read the header and write it
        return cleanup_file(file_state.text)

    def write_directory(self, directory_path: str) -> str
        files = os.listdir(directory_path)
        files.sort()
        text = ""
        for fname in files:
            if fname in excluded_files:
                continue
            # print(fname)
            fpath = os.path.join(directory_path, fname)
            if os.path.isdir(fpath):
                text += self.write_directory(fpath)
            elif fname.endswith('.cpp') or fname.endswith('.c') or fname.endswith('.cc'):
                text += self.write_file(fpath)
        return text

def generate_duckdb_hpp(header_file):
    print("-----------------------")
    print("-- Writing " + header_file + " --")
    print("-----------------------")
    with open_utf8(temp_header, 'w+') as hfile:
        hfile.write("/*\n")
        hfile.write(write_file("LICENSE"))
        hfile.write("*/\n\n")

        hfile.write("#pragma once\n")
        hfile.write("#define DUCKDB_AMALGAMATION 1\n")
        if extended_amalgamation:
            hfile.write("#define DUCKDB_AMALGAMATION_EXTENDED 1\n")
        hfile.write("#define DUCKDB_SOURCE_ID \"%s\"\n" % git_commit_hash())

        dev_version = git_dev_version()
        dev_v_parts = dev_version.lstrip('v').split('.')
        hfile.write("#define DUCKDB_VERSION \"%s\"\n" % dev_version)
        hfile.write("#define DUCKDB_MAJOR_VERSION %d\n" % int(dev_v_parts[0]))
        hfile.write("#define DUCKDB_MINOR_VERSION %d\n" % int(dev_v_parts[1]))
        hfile.write("#define DUCKDB_PATCH_VERSION \"%s\"\n" % dev_v_parts[2])

        for fpath in main_header_files:
            hfile.write(write_file(fpath))


def generate_amalgamation(source_file, header_file):
    # construct duckdb.hpp from these headers
    generate_duckdb_hpp(header_file)

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


def gather_file(current_file, source_files, header_files):
    global linenumbers
    global written_files
    if not need_to_write_file(current_file, False):
        return ""
    written_files.add(current_file)


    (statements, includes) = get_includes(current_file, text)
    # find the linenr of the final #include statement we parsed
    if len(statements) > 0:
        index = text.find(statements[-1])
        linenr = len(text[:index].split('\n'))

        # now write all the dependencies of this header first
        for i in range(len(includes)):
            # source file inclusions are inlined into the main text
            include_text = write_file(includes[i])
            if linenumbers and i == len(includes) - 1:
                # for the last include statement, we also include a #line directive
                include_text += '\n#line %d "%s"\n' % (linenr, current_file)
            if includes[i].endswith('.cpp') or includes[i].endswith('.cc') or includes[i].endswith('.c'):
                # source file inclusions are inlined into the main text
                text = text.replace(statements[i], include_text)
            else:
                text = text.replace(statements[i], '')
                header_files.append(include_text)

    # add the initial line here
    if linenumbers:
        text = '\n#line 1 "%s"\n' % (current_file,) + text
    source_files.append(cleanup_file(text))


def gather_files(dir, source_files, header_files):
    files = os.listdir(dir)
    files.sort()
    for fname in files:
        if fname in excluded_files:
            continue
        fpath = os.path.join(dir, fname)
        if os.path.isdir(fpath):
            gather_files(fpath, source_files, header_files)
        elif fname.endswith('.cpp') or fname.endswith('.c') or fname.endswith('.cc'):
            gather_file(fpath, source_files, header_files)


def write_license(hfile):
    hfile.write("// See https://raw.githubusercontent.com/duckdb/duckdb/main/LICENSE for licensing information\n\n")


def generate_amalgamation_splits(source_file, header_file, nsplits):
    # construct duckdb.hpp from these headers
    generate_duckdb_hpp(header_file)

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

    partition_fnames = []
    current_partition = 0
    for partition in partitions:
        partition_name = source_file.replace('.cpp', '-%s.cpp' % (partition_names[current_partition],))
        temp_partition_name = partition_name + '.tmp'
        partition_fnames.append([partition_name, temp_partition_name])
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
        current_partition += 1

    copy_if_different(temp_header, header_file)
    copy_if_different(temp_internal_header, internal_header_file)
    try:
        os.remove(temp_header)
        os.remove(temp_internal_header)
    except:
        pass
    for p in partition_fnames:
        copy_if_different(p[1], p[0])
        try:
            os.remove(p[1])
        except:
            pass

def configure_parser():
    import argparse
    parser = argparse.ArgumentParser(description='Create a single header + source file combination out of the DuckDB sources')
    parser.add_argument(
        '--extended', 
        action='store_true',
        help='Generate extended amalgamation with additional header files',
        default=False
    )
    parser.add_argument(
        '--linenumbers',
        action='store_true',
        help='Include line numbers in the amalgamation',
        default=False
    )
    parser.add_argument(
        '--no-linenumbers',
        action='store_false',
        dest='linenumbers',
        help='Exclude line numbers from the amalgamation'
    )
    parser.add_argument(
        '--header',
        help='Specify header file path',
        default=header_file
    )
    parser.add_argument(
        '--source',
        help='Specify source file path',
        default=source_file
    )
    parser.add_argument(
        '--splits',
        type=int,
        help='Number of splits for the amalgamation',
        default=1
    )
    parser.add_argument(
        '--list-sources',
        action='store_true',
        help='List all source files'
    )
    parser.add_argument(
        '--list-objects',
        action='store_true',
        help='List all object files'
    )
    parser.add_argument(
        '--includes',
        action='store_true',
        help='List include directories with -I prefix'
    )
    parser.add_argument(
        '--include-directories',
        action='store_true',
        help='List include directories'
    )
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

    amalgamator = Amalgamator(header_file=header_file, source_file=source_file, extended=extended, linenumbers=linenumbers)
    amalgamtor.initialize()

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

    # Create output directory
    if os.path.exists(output_directory):
        shutil.rmtree(output_directory)
    os.makedirs(output_directory)

    amalgamator.gather_headers()

    # Generate amalgamation
    if args.splits > 1:
        generate_amalgamation_splits(source_file, header_file, args.splits)
    else:
        generate_amalgamation(source_file, header_file)

if __name__ == "__main__":
    main()
