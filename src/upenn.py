import os
# import csv
# import sys
import shutil

from download.box import LifespanBox

verbose = True

root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
processed_file = os.path.join(root_dir, 'store/processed-ksads.txt')
cache_space = os.path.join(root_dir, 'cache', 'ksads')

behavioral_folder_id = 0
box = LifespanBox(cache=cache_space)

assessments = {}


def main():
    # Clean up cache space
    shutil.rmtree(box.cache)


def get_all_rows():
    rows = []
    return rows


if __name__ == '__main__':
    main()
