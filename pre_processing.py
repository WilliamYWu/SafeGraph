from global_constants import *

import os
import pandas as pd
import psutil
import logging
import logging.handlers
import gc
import time
import ast

def processing_dataset(filepaths, output_directory, need_processing):
    logger = logging.getLogger('app')
    try:
        if need_processing:
            start = time.time()
            logger.info(f"Time Taken: {time.time()-start} seconds")
            for file in filepaths:
                name_split = file_name_preprocessing(file)
                file_output = output_directory + f'\\{name_split[0]}-{name_split[1]}-{name_split[3]}.csv'
                raw_df = pd.read_csv(file)
                raw_df['source_location-year'] = name_split[0]
                raw_df['source_location-month'] = name_split[1]
                raw_df['source_location-part'] = name_split[2]
                normal_dict_preprocess_list = ['spend_per_transaction_percentiles', 
                                                'spend_by_day_of_week', 
                                                'day_counts', 
                                                'transaction_intermediary', 
                                                'spend_by_transaction_intermediary', 
                                                'bucketed_customer_frequency', 
                                                'mean_spend_per_customer_by_frequency', 
                                                'bucketed_customer_incomes', 
                                                'mean_spend_per_customer_by_income', 
                                                'customer_home_city']

                raw_df = dictionary_preprocess(raw_df, normal_dict_preprocess_list)
                raw_df.to_csv(file_output)
    except Exception as e:
        logging.error(f"Failed to process dataset: {e}")

def file_name_preprocessing(file_name):
    split_dir = file_name.split('\\')
    year = split_dir[-3].replace('y=','')
    month = split_dir[-2].replace('m=','')
    part = split_dir[-1].split('-tid-')[0]
    return year, month, part

def dictionary_preprocess(dataframe, column_name_list):
    for column_name in column_name_list:
        df = dataframe[column_name]
        if column_name == 'open_hours':
            null_open = '{ "Mon": [["0:00", "0:00"]], "Tue": [["0:00", "0:00"]], "Wed": [["0:00", "0:00"]], "Thu": [["0:00", "0:00"]], "Fri": [["0:00", "0:00"]], "Sat": [["0:00", "0:00"]], "Sun": [["0:00", "0:00"]]}' 
            df = df.apply(lambda x: x if x == x else null_open)
        df = df.apply(lambda x: ast.literal_eval(x))
        df = df.apply(pd.Series)
        column_list = df.columns
        new_names = []
        for name in column_list:
            new_names.append(f"{column_name}-{name}")
        df.columns = new_names
        if column_name == 'open_hours':
            df = nested_list_preprocess(df)
        dataframe = pd.concat([dataframe.drop([column_name],axis=1),df], axis=1)
    return dataframe

def nested_list_preprocess(df):
    new_df = rename_new_columns(df, new_df, column)
    for column in new_df.columns:
        new_df[column].loc[new_df[column].isnull()] = new_df[column].loc[new_df[column].isnull()].apply(lambda x: ['0:00', '0:00'])
    newer_df = rename_new_columns(new_df, newer_df, column)
    return newer_df

def rename_new_columns(df):
    new_df = pd.DataFrame()
    for column in df.columns:
        rename_list = []
        split_df = pd.DataFrame(df[column].to_list(), index=df.index)
        if 'open_house' not in column:
            for index, new_column in enumerate(split_df.columns):
                rename_list.append(f"{column}-{index+1}")
        else:
            rename_list.append(f"{column}-start")
            rename_list.append(f"{column}-end")
        split_df.columns = rename_list
        new_df = pd.concat([new_df, split_df],axis=1)
    return new_df

def open_hours_dictionary_preprocess(dataframe):
    df = dataframe['open_hours']
    df = df.apply(lambda x: ast.literal_eval(x))
    df = df.apply(pd.Series)
    column_list = df.columns
    new_names = []
    for name in column_list:
        new_names.append(f"'open_hours'-{name}")
    df.columns = new_names
