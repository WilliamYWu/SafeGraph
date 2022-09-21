from global_constants import *
from pre_processing import *

from concurrent.futures import ThreadPoolExecutor
import logging
import logging.handlers
import os
import pandas as pd
import gzip
import shutil
import warnings

# region: functions
def suppress_warnings():
    warnings.filterwarnings("ignore")

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
    current_folder_dir = file.rsplit('\\',maxsplit=1)[0]
    os.chdir(current_folder_dir)
    without_gz_file_name = file.replace(".gz",'')
    with gzip.open(file, 'r') as file_in:
        with open(without_gz_file_name, 'wb') as file_out: 
            shutil.copyfileobj(file_in, file_out)
        file_out.close()
    file_in.close()
    
@timeit_memoryusage
def unzip_all_gz(paths, run):
    if run:
        with ThreadPoolExecutor(max_workers=10) as pool:
            result = pool.map(unzip_one_gz, paths)

def main():
    suppress_warnings()
    dir_list = [LOG_DIR, CLEANED_DATA_DIR, STAGING_0_INPUT, STAGING_1_INPUT, STAGING_1_OUTPUT, STAGING_2_OUTPUT, STAGING_3_INPUT]
    directory_setup(dir_list)
    logging_setup()
    run_unzip_gz = False
    preprocess = True

    gz_paths = create_file_list(SPEND_PATTERNS, ".gz")
    unzip_all_gz(gz_paths, run_unzip_gz)
    raw_csv_list = create_file_list(SPEND_PATTERNS, '.csv')
    processing_all_files(raw_csv_list, STAGING_0_INPUT, preprocess)
    logging.info("Done")

if __name__ == "__main__":
    main()