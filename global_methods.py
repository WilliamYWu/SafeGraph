from global_constants import *

import gc
import time
import psutil
import logging
import logging.handlers
from functools import wraps 
import os
import pandas as pd

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

@timeit_memoryusage
def create_file_list(parent_dir, filetype):
    paths = []
    for root, dirs, files in os.walk(parent_dir):
        for file in files:
            if file.endswith(filetype):
                paths.append(os.path.join(root,file))
    logging.info(f"Number of Files: {len(paths)}")
    return paths