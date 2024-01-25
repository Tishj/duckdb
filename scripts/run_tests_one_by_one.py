import sys
import subprocess
import re
import os
import time

import argparse

parser = argparse.ArgumentParser(description='Run tests one by one with optional flags.')
parser.add_argument('unittest_program', help='Path to the unittest program')
parser.add_argument('--no-exit', action='store_true', help='Do not exit after running tests')
parser.add_argument('--profile', action='store_true', help='Enable profiling')
parser.add_argument('--no-assertions', action='store_false', help='Disable assertions')
parser.add_argument('--time_execution', action='store_true', help='Measure and print the execution time of each test')
parser.add_argument('--verbose', action='store_true', help='Always output stdout and stderr of tests run')
parser.add_argument('--timeout', type=int, default=None, help='Maximum time for a test to run before it is killed')
parser.add_argument('--success', action='store_true', help='Include successful tests in output')

args, extra_args = parser.parse_known_args()

if not args.unittest_program:
    parser.error('Path to unittest program is required')

# Access the arguments
unittest_program = args.unittest_program
no_exit = args.no_exit
profile = args.profile
assertions = args.no_assertions
time_execution = args.time_execution

# Use the '-l' parameter to output the list of tests to run
proc = subprocess.Popen([unittest_program, '-l'] + extra_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
stdout = proc.stdout.read().decode('utf8')
stderr = proc.stderr.read().decode('utf8')
if proc.returncode is not None and proc.returncode != 0:
    print("Failed to run program " + unittest_program)
    print(proc.returncode)
    print(stdout)
    print(stderr)
    exit(1)

# The output is in the format of 'PATH\tGROUP', we're only interested in the PATH portion
test_cases = []
first_line = True
for line in stdout.splitlines():
    if first_line:
        first_line = False
        continue
    if len(line.strip()) == 0:
        continue
    splits = line.rsplit('\t', 1)
    test_cases.append(splits[0])


test_count = len(test_cases)
return_code = 0


def parse_assertions(stdout):
    for line in stdout.splitlines():
        if line == 'assertions: - none -':
            return "0 assertions"

        # Parse assertions in format
        pos = line.find("assertion")
        if pos != -1:
            space_before_num = line.rfind(' ', 0, pos - 2)
            return line[space_before_num + 2 : pos + 10]

    return ""


test_extra_args = []
if args.success:
    test_extra_args.append('--success')

for _ in range(10):
    for test_number, test_case in enumerate(test_cases):
        if not profile:
            print(f"[{test_number}/{test_count}]: {test_case}", end="")
        start = time.time()
        try:
            arguments = [unittest_program, test_case] + test_extra_args
            res = subprocess.run(arguments, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=args.timeout)
            stdout = res.stdout.decode('utf8')
            stderr = res.stderr.decode('utf8')
            success = res.returncode is not None and res.returncode != 0
            return_code = res.returncode
        except subprocess.TimeoutExpired as e:
            stdout = e.stdout.decode('utf8')
            # stderr = e.stderr.decode('utf8')
            success = False
            return_code = 124
        end = time.time()

        additional_data = ""
        if assertions:
            additional_data += " (" + parse_assertions(stdout) + ")"
        if args.time_execution:
            additional_data += f" (Time: {end - start:.4f} seconds)"

        print(additional_data, flush=True)
        if profile:
            print(f'{test_case}	{end - start}')
        if args.verbose:
            print("STDOUT:")
            print(stdout)
            print("STDERR:")
            print(stderr)
        else:
            if success:
                print("FAILURE IN RUNNING TEST")
                print(
                    """--------------------
        RETURNCODE
        --------------------
        """
                )
                print(return_code)
                print(
                    """--------------------
        STDOUT
        --------------------
        """
                )
                print(stdout)
                print(
                    """--------------------
        STDERR
        --------------------
        """
                )
                print(stderr)
                return_code = 1
                if not no_exit:
                    break


exit(return_code)
