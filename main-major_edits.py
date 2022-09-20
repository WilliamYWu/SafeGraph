from global_constants import *

import time
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from functools import wraps 
from itertools import repeat
import logging
import logging.handlers
import psutil
import os
import gc
import pandas as pd
import gzip
import shutil

# region: quality of life
def timeit_memoryusage(method):
    @wraps(method)
    def wrapper(*args, **kwargs):
        process = psutil.Process(os.getpid())
        start_memory = process.memory_info().rss
        start_time = time.time()
        result = method(*args, **kwargs)
        end_time = time.time()
        end_memory = process.memory_info().rss
        logging.info(f"{method.__name__} Time Taken => {(end_time-start_time)*1000} ms")
        logging.info(f"{method.__name__} Memory Used => {(end_memory-start_memory)} bytes")
        return result
    return wrapper

def clean(df):
    del df
    gc.collect()
    df = pd.DataFrame()
    return df

def directory_setup(dir_list):
    for directory in dir_list:
        if not os.path.exists(directory):
            os.makedirs(directory)

def logging_setup():
    try:
      handler = logging.handlers.WatchedFileHandler(os.environ.get("LOGFILE", LOG_FILE))
      formatter = logging.Formatter(fmt="%(asctime)s %(levelname)-8s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
      handler.setFormatter(formatter)
      logging.getLogger().handlers.clear()
      root = logging.getLogger()
      root.setLevel(os.environ.get("LOGLEVEL", "INFO"))
      root.addHandler(handler)
      logging.propogate = False
      logging.info("Log File was created successfully.")
    except Exception as e:
        exit
# endregion: quality of life

@timeit_memoryusage
def create_file_list(parent_dir, filetype):
    paths = []
    for root, dirs, files in os.walk(parent_dir):
        for file in files:
            if file.endswith(filetype):
                paths.append(os.path.join(root,file))
    logging.info(f"Number of Files: {len(paths)}")
    return paths

def unzip_one_gz(file):
    without_gz_file_name = file.replace(".gz",'')
    shutil.copy(file, without_gz_file_name)

@timeit_memoryusage
def unzip_all_gz(paths, run):
    if run:
        with ThreadPoolExecutor(max_workers=10) as pool:
                pool.map(unzip_one_gz, paths)

def main():
    dir_list = [LOG_DIR, CLEANED_DATA_DIR, STAGING_0_INPUT, STAGING_1_INPUT, STAGING_1_OUTPUT, STAGING_2_OUTPUT, STAGING_3_INPUT]
    directory_setup(dir_list)

    run_unzip_gz = True
    preprocess = True
    convert = True
    run_naics = True
    run_groupby_naics = True

    gz_paths = create_file_list(SPEND_PATTERNS, ".gz")
    unzip_all_gz(gz_paths, run_unzip_gz)
    raw_csv_list = create_file_list(SPEND_PATTERNS, '.csv')
    logging.info("Done")

logging_setup()
if __name__ == "__main__":
    main()