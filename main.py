from global_constants import *
from global_methods import *
from pre_processing import *
from safegraph_db_connection import *

from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from itertools import repeat
import logging
import logging.handlers
import warnings
import mysql.connector as mysql

# region: functions
def suppress_warnings():
    warnings.filterwarnings("ignore")

@timeit_memoryusage
def unzip_all_gz(paths, run):
    if run:
        with ThreadPoolExecutor(max_workers=10) as pool:
            result = pool.map(unzip_one_gz, paths)

@timeit_memoryusage
def processing_all_files(paths, output_directory, run):
    if run:
        with ProcessPoolExecutor(max_workers=10) as pool:
            result = pool.map(processing_one_file, paths, repeat(output_directory))

@timeit_memoryusage
def insert_database_threading(file_list, table, run):
    if run:
        with ThreadPoolExecutor(max_workers=10) as pool:
            result = pool.map(from_file_populate_table, file_list, repeat(table))

def main():
    suppress_warnings()
    dir_list = [LOG_DIR, CLEANED_DATA_DIR, STAGING_1_INPUT]
    directory_setup(dir_list)
    logging_setup()
    run_unzip_gz = False
    preprocess = False
    insert_raw_data = True

    gz_paths = create_file_list(SPEND_PATTERNS, ".gz")
    unzip_all_gz(gz_paths, run_unzip_gz)
    raw_csv_list = create_file_list(SPEND_PATTERNS, '.csv')
    processing_all_files(raw_csv_list, STAGING_1_INPUT, preprocess)
    logging.info("Done")

    create_database()
    create_raw_table()
    insert_database_threading(raw_csv_list, "raw_data", insert_raw_data)

if __name__ == "__main__":
    main()