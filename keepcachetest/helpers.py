import atexit

import shutil
from tempfile import mkdtemp

_temp_directories = []


def _remove_temp_directories():
    for directory in _temp_directories:
        shutil.rmtree(directory)


def create_temp_directory():
    temp_directory = mkdtemp()
    _temp_directories.append(temp_directory)
    return temp_directory


atexit.register(_remove_temp_directories)
