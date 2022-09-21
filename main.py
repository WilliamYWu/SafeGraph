from global_constants import *
from global_methods import *
from pre_processing import *

from concurrent.futures import ThreadPoolExecutor
import logging
import logging.handlers
import warnings

# region: functions
def suppress_warnings():
    warnings.filterwarnings("ignore")

def main():
    suppress_warnings()
    dir_list = [LOG_DIR, CLEANED_DATA_DIR, STAGING_1_INPUT]
    directory_setup(dir_list)
    logging_setup()
    run_unzip_gz = False
    preprocess = False

    gz_paths = create_file_list(SPEND_PATTERNS, ".gz")
    unzip_all_gz(gz_paths, run_unzip_gz)
    raw_csv_list = create_file_list(SPEND_PATTERNS, '.csv')
    processing_all_files(raw_csv_list, STAGING_1_INPUT, preprocess)
    logging.info("Done")

if __name__ == "__main__":
    main()