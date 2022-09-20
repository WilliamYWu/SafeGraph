from pre_processing import *
from global_constants import *

import os
import gzip
import shutil
import pandas as pd
import logging
import logging.handlers
import pyarrow as pa
import pyarrow.parquet as pq
import time
import concurrent.futures
import sys
import fastai.tabular.core
import warnings


# region: functions
def suppress_warnings():
    warnings.filterwarnings("ignore")


def unzip_gz_files(dir, filetype, unzip_gz):
    logger = logging.getLogger('app')
    try:
        if unzip_gz:
            start = time.time()
            logger.info(f"MEMORY USAGE -> START -> unzip_gz_files: {memory_usage()}")
            logger.info('Starting to unzip files.')
            files_list = create_file_list(dir, filetype)
            for file in files_list:
                with gzip.open(file, 'r') as file_in:
                    without_gz_file_name = file.rsplit('\\', maxsplit=1)[1].replace('.gz','')
                    with open(without_gz_file_name, 'wb') as file_out: 
                        current_folder_dir = file.rsplit('\\',maxsplit=1)[0]
                        os.chdir(current_folder_dir)
                        shutil.copyfileobj(file_in, file_out)
            logger.info('Successfully unzipped files.')
            logger.info(f"Time Taken: {time.time()-start} seconds")
            logger.info(f"MEMORY USAGE -> END -> unzip_gz_files: {memory_usage()}")
    except Exception as e:
        logger.error(f'Failed to unzip files: {e}')

def create_file_list(filepath, filetype):
    logger = logging.getLogger('app')
    start = time.time()
    logger.info(f"MEMORY USAGE -> START -> create_file_list: {memory_usage()}")
    paths = []
    for root, dirs, files in os.walk(filepath):
        for file in files:
            if file.lower().endswith(filetype.lower()):
                paths.append(os.path.join(root, file))
    logger.info(f"Number of Files: {len(paths)}")
    logger.info(f"Time Taken: {time.time()-start} seconds")
    logger.info(f"MEMORY USAGE -> END -> create_file_list: {memory_usage()}")
    return paths

def convert_to_parquet(spend_patterns_list, convert):
    logger = logging.getLogger('app')
    if convert:
        try:
            start = time.time()
            logger.info(f"MEMORY USAGE -> START -> convert_to_parquet: {memory_usage()}")
            for file in spend_patterns_list:
                file_name = file.rsplit('\\')[-1].replace('.csv','')
                staging_1_dir = f"{STAGING_1_INPUT}\\{file_name}.parquet"
                csv_df = pd.read_csv(file, index_col=False)
                parquet_df = pa.Table.from_pandas(csv_df, preserve_index=False)
                csv_df = clean(csv_df)
                pq.write_table(parquet_df, staging_1_dir)
                parquet_df = clean(parquet_df)
            logger.info(f"Time Taken: {time.time()-start} seconds")
            logger.info(f"MEMORY USAGE -> END -> convert_to_parquet: {memory_usage()}")
        except Exception as e:
            logger.error(f"Failed to convert to parquet: {e}")

def naics_filter_dataset(filelist, column_filter, output_path, start_filter):
    logger = logging.getLogger('app')
    if start_filter:
        naics_df = pd.DataFrame()
        unique_naics_list = []
        unique_naics_df = pd.DataFrame()
        try:
            start = time.time()
            logger.info(f"MEMORY USAGE -> START -> naics_filter_dataset: {memory_usage()}")
            needed_columns = columns_from_dict(column_filter)
            for file in filelist:
                partial_df = pd.read_parquet(file, columns=needed_columns)
                naics_df['naics_code'] = partial_df['naics_code'].unique()
                naics_df['naics_code'] = naics_df['naics_code'].fillna(0)
                for naics in naics_df['naics_code']:
                    if naics not in unique_naics_list:
                        unique_naics_list.append(naics)
                naics_df = clean(partial_df)
                partial_df = clean(partial_df)
            unique_naics_df['naics_code'] = unique_naics_list         
            table = pa.Table.from_pandas(unique_naics_df)
            unique_naics_df = clean(unique_naics_df)
            pq.write_table(table, output_path)
            logger.info(f"Time Taken: {time.time()-start} seconds")
            logger.info(f"MEMORY USAGE -> END -> naics_filter_dataset: {memory_usage()}")
            return output_path
        except Exception as e:
            logger.error(f"Failed to read naics: {e}")

def columns_from_dict(columnfilter):
    keys_list = [] 
    for keys, value in columnfilter.items():
        keys_list.append(keys)   
    return keys_list

def groupby_naics(naics_path, staging_path, parquet_files, group_naics):
    logger = logging.getLogger('app')
    try:
        start = time.time()
        logger.info(f"MEMORY USAGE -> START -> groupby_naics: {memory_usage()}")
        if group_naics:
            naics_df = pd.read_parquet(naics_path)
            for naics in naics_df['naics_code']:
                view_df = pd.DataFrame()
                count = 0
                output_dir = staging_path+ f'\\{int(naics)}' 
                if not os.path.exists(output_dir):
                    os.makedirs(output_dir)
                for file in parquet_files:
                    raw_df = pd.read_parquet(file)
                    raw_df = fastai.tabular.core.df_shrink(raw_df[(raw_df['naics_code'] == int(naics))])
                    view_df = pd.concat([view_df, raw_df])
                    raw_df = clean(raw_df)
                    limit_memory = 150000000
                    output_path = output_dir + f'\\{int(naics)}_{count}.parquet'
                    print(sys.getsizeof(view_df))    
                    if sys.getsizeof(view_df) >= limit_memory or file == parquet_files[-1]:
                        table = pa.Table.from_pandas(view_df)
                        count += 1
                        view_df = clean(view_df)
                        pq.write_table(table, output_path)
        logger.info(f"Time Taken: {time.time()-start} seconds")
        logger.info(f"MEMORY USAGE -> END -> groupby_naics: {memory_usage()}")
    except Exception as e:
        logger.error(f"Failed to groupby naics: {e}")

def filter_dataset(naics_list, column_filter, should_filter):
    logger = logging.getLogger('app')
    try:
        if should_filter:
            start = time.time()
            logger.info(f"Time Taken: {time.time()-start} seconds")
            logger.info(f"MEMORY USAGE -> START -> filter_dataset: {memory_usage()}")
            
            logger.info(f"MEMORY USAGE -> END -> filter_dataset: {memory_usage()}")
    except Exception as e:
        logging.error(f"Failed to filter dataset: {e}")
    return



# endregion: functions

def main():
    run_unzip_gz = False
    preprocess = True
    convert = True
    run_naics = True
    run_groupby_naics = True

    suppress_warnings()
    # unzip_gz_files(MAIN_DIR, '.gz', run_unzip_gz)
    # raw_csv_list = create_file_list(SPEND_PATTERNS, '.csv')
    # processing_dataset(raw_csv_list, STAGING_0_INPUT, preprocess)
    # print('Done')
    # processing_dataset(raw_csv_list, STAGING_0_INPUT, preprocess)
    # processed_csv_list = create_file_list(STAGING_0_INPUT, '.csv')[0]
    # convert_to_parquet,[(processed_csv_list, convert)
    # parquet_file_list = create_file_list(STAGING_1_INPUT,'.parquet')[0]
    # naics_output_path = STAGING_1_OUTPUT + '\\naics_list.parquet'
    # naics_filter_dataset(parquet_file_list, {'naics_code':'category'}, naics_output_path, run_naics))
    # pool.starmap(groupby_naics,[(naics_output_path, STAGING_2_OUTPUT, parquet_file_list, run_groupby_naics)])
    # print('Script is done')

dir_list = [CLEANED_DATA_DIR, STAGING_0_INPUT, STAGING_1_INPUT, STAGING_1_OUTPUT, STAGING_2_OUTPUT, STAGING_3_INPUT]
directory_setup(dir_list)
logging_setup()
if __name__ == '__main__':
    main()