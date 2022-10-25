from global_constants import *
from global_methods import *


import os
import pandas as pd
import logging
import logging.handlers
import re
import gzip
import shutil
import json

# TODO: This doesn't work perfectly yet for some reason. It looks like it saves the first file of every new month in the wrong folder
def unzip_one_gz(file):
    current_folder_dir = file.rsplit('\\',maxsplit=1)[0]
    os.chdir(current_folder_dir)
    without_gz_file_name = file.replace(".gz",'')
    with gzip.open(file, 'r') as file_in:
        with open(without_gz_file_name, 'wb') as file_out: 
            shutil.copyfileobj(file_in, file_out)
        file_out.close()
    file_in.close()

def processing_one_file(file, output_directory):
    try:
        name_split = file_name_preprocessing(file)
        file_output = output_directory + f'\\{name_split[0]}-{name_split[1]}-{name_split[2]}.csv'
        raw_df = pd.read_csv(file)
        raw_df['source_location-year'] = name_split[0]
        raw_df['source_location-month'] = name_split[1]
        raw_df['source_location-part'] = name_split[2]
        # WARNING: Temporarily removed 'customer_home_city', 'bucketed_customer_frequency', 'bucketed_customer_incomes',
        # TODO: For all of these fields it would be better to split the dicitonary into two seperate columns on of which is called the "Field-Name" and the other is called "Field-Value"
        normal_dict_preprocess_list = [ 'open_hours',
                                        'spend_per_transaction_percentiles', 
                                        'spend_by_day_of_week', 
                                        'day_counts', 
                                        'transaction_intermediary', 
                                        'spend_by_transaction_intermediary', 
                                        'mean_spend_per_customer_by_frequency', 
                                        'mean_spend_per_customer_by_income']                                      
        raw_df = dictionary_preprocess(raw_df, normal_dict_preprocess_list)
        raw_df.to_csv(file_output)
        raw_df = clean(raw_df)
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
        df = df.apply(lambda x: json.loads(x))
        df = df.apply(pd.Series)
        column_list = df.columns
        new_names = []
        for name in column_list:
            new_names.append(f"{column_name}-{name}")
        df.columns = new_names
        if column_name == 'open_hours':
            df = nested_list_preprocess(df)
            df = hours_worked(df)
        dataframe = pd.concat([dataframe.drop([column_name],axis=1),df], axis=1)
    return dataframe.fillna(0)

def hours_worked(df):
    start_end_lists = string_in_list(df, "start")
    start_list = start_end_lists[0]
    end_list = start_end_lists[1]
    for i in range(0, len(start_list)):
        day_metadata = start_list[i].split('-')
        day_index = day_metadata[1] + day_metadata[2]
        df[day_index] = df[end_list[i]] - df[start_list[i]]
        df.drop([end_list[i], start_list[i]], inplace=True, axis=1)
    one_two_lists = string_in_list(df, "1")
    one_list = one_two_lists[0]
    two_list = one_two_lists[1]
    for i in range(0, len(one_list)):
        day = "open_hours-" + re.sub('\d+', '', one_list[i])
        df[day] = df[one_list[i]] + df[two_list[i]]
        df.drop([one_list[i], two_list[i]], inplace=True, axis=1)
    return df

def string_in_list(df, word):
    in_list = []
    out_list = []
    for column in df.columns:
        if word == "start":
            df[column] = df[column].apply(lambda x: int(x.split(':')[0]))
        if word in column:
            in_list.append(column)
        else:
            out_list.append(column)
    return [in_list, out_list]

def nested_list_preprocess(df):
    new_df = rename_new_columns(df, True)
    # NOTE: Replace everything where new_df is null
    for column in new_df.columns:
        new_df[column].loc[new_df[column].isnull()] = new_df[column].loc[new_df[column].isnull()].apply(lambda x: ['0:00', '0:00'])
    newer_df = rename_new_columns(new_df, False)
    return newer_df

def rename_new_columns(df, still_nested):
    new_df = pd.DataFrame()
    for column in df.columns:
        rename_list = []
        split_df = pd.DataFrame(df[column].to_list())
        if still_nested:
            for index, new_column in enumerate(split_df.columns):
                rename_list.append(f"{column}-{index+1}")
        else:
            rename_list.append(f"{column}-start")
            rename_list.append(f"{column}-end")
        split_df.columns = rename_list
        new_df = pd.concat([new_df, split_df],axis=1)
    return new_df