import os

######
# MAIN_BRANCH_VERSIONING default value needs to keep in sync between:
# - CMakeLists.txt
# - scripts/amalgamation.py
# - scripts/package_build.py
# - tools/pythonpkg/setup.py
######
MAIN_BRANCH_VERSIONING = False if os.getenv('MAIN_BRANCH_VERSIONING') == "0" else True


def get_git_describe():
    import subprocess

    override_git_describe = os.getenv('OVERRIDE_GIT_DESCRIBE') or ''
    versioning_tag_match = 'v*.*.*'
    if MAIN_BRANCH_VERSIONING:
        versioning_tag_match = 'v*.*.0'
    # empty override_git_describe, either since env was empty string or not existing
    # -> ask git (that can fail, so except in place)
    if len(override_git_describe) == 0:
        try:
            return (
                subprocess.check_output(
                    ['git', 'describe', '--tags', '--long', '--debug', '--match', versioning_tag_match]
                )
                .strip()
                .decode('utf8')
            )
        except subprocess.CalledProcessError:
            return "v0.0.0-0-gdeadbeeff"
    if len(override_git_describe.split('-')) == 3:
        return override_git_describe
    if len(override_git_describe.split('-')) == 1:
        override_git_describe += "-0"
    assert len(override_git_describe.split('-')) == 2
    try:
        return (
            override_git_describe
            + "-g"
            + subprocess.check_output(['git', 'log', '-1', '--format=%h']).strip().decode('utf8')
        )
    except subprocess.CalledProcessError:
        return override_git_describe + "-g" + "deadbeeff"


def git_dev_version():
    def prefix_version(version):
        """Make sure the version is prefixed with 'v' to be of the form vX.Y.Z"""
        if version.startswith('v'):
            return version
        return 'v' + version

    if 'SETUPTOOLS_SCM_PRETEND_VERSION' in os.environ:
        return prefix_version(os.environ['SETUPTOOLS_SCM_PRETEND_VERSION'])
    try:
        long_version = get_git_describe()
        version_splits = long_version.split('-')[0].lstrip('v').split('.')
        dev_version = long_version.split('-')[1]
        if int(dev_version) == 0:
            # directly on a tag: emit the regular version
            return "v" + '.'.join(version_splits)
        else:
            # not on a tag: increment the version by one and add a -devX suffix
            # this needs to keep in sync with changes to CMakeLists.txt
            if MAIN_BRANCH_VERSIONING == True:
                # increment minor version
                version_splits[1] = str(int(version_splits[1]) + 1)
            else:
                # increment patch version
                version_splits[2] = str(int(version_splits[2]) + 1)
            return "v" + '.'.join(version_splits) + "-dev" + dev_version
    except:
        return "v0.0.0"


def open_utf8(fpath, flags):
    import sys

    if sys.version_info[0] < 3:
        return open(fpath, flags)
    else:
        return open(fpath, flags, encoding="utf8")


def normalize_path(path):
    import os

    def normalize(p):
        return os.path.sep.join(p.split('/'))

    if isinstance(path, list):
        normed = map(lambda p: normalize(p), path)
        return list(normed)

    if isinstance(path, str):
        return normalize(path)

    raise Exception("Can only be called with a str or list argument")
