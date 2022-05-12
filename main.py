import logging
import time
import os
import numpy as np
import pandas as pd

import params
from constants import input_csv_dir_path, output_parquet_dir_path

pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_columns', None)


def read_csv(input_path):
    try:
        weather_df = pd.read_csv(input_path, sep=',')
    except IOError:
        err_msg = 'cannot open the csv file provided in the input path: %s' % input_path
        logging.info(err_msg)
        raise Exception(err_msg)
    return weather_df


def write_parquet(df, output_path):
    try:
        df.to_parquet(output_path, row_group_size=1000)
        return 1
    except Exception as e:
        err_msg = f"{e} - in writing parquet file to the output path: {output_path}"
        logging.info(err_msg)
        raise Exception(err_msg)


def merge_dfs(df_list):
    try:
        return pd.concat(df_list, ignore_index=True)
    except Exception as e:
        err_msg = f'Error in concatenating Dataframes - {e}'
        logging.info(err_msg)
        raise Exception(err_msg)


def process_files(weather_df, output_path):
    """
    Function to process the Dataframes generated from CSV files into Parquet file.
    :param weather_df: Input DataFrame generated from CSV files
    :param output_path: Output Directory where Parquet file will be generated.
    :return: True or False
    """
    try:
        # Renaming column names from mixed case letters(sentence case) to lower case with underscore.
        weather_df.rename(columns=params.rename_columns_dict, inplace=True)

        # Converting date field with different format to pandas/SQL date format
        weather_df[
            'observation_date'
        ] = pd.to_datetime(weather_df['observation_date'], format='%Y-%m-%dT%H:%M:%S').dt.date

        # Replace Null values to zero for float fields
        weather_df['screen_temperature'] = weather_df['screen_temperature'].fillna(0.0)
        weather_df['pressure'] = weather_df['pressure'].fillna(0.0)

        # Replacing country field with corresponding value if it is NUll by using region field
        reg_cty_df = weather_df[['region', 'country']].where(weather_df['country'].notnull()).drop_duplicates()
        weather_df = pd.merge(weather_df, reg_cty_df, how='left', on='region', suffixes=('', '_remove'))
        weather_df['country'] = np.where(weather_df['country'].isnull(), weather_df['country_remove'],
                                         weather_df['country'])
        weather_df.drop([i for i in weather_df.columns if 'remove' in i], axis=1, inplace=True)

        # Adding screen_temperature_day_max field to capture daily max temperature
        weather_df[
            'screen_temperature_day_max'
        ] = weather_df.groupby(['observation_date'])['screen_temperature'].transform(np.max)

        # Adding screen_temperature_monthly_rank field to capture monthly rank of a day in terms of the
        # highest temperature
        weather_df[
            'screen_temperature_monthly_rank'
        ] = weather_df['screen_temperature_day_max'].rank(method='dense', ascending=False).astype(int)

        return write_parquet(weather_df, output_path)

    except Exception as e:
        err_msg = f'Error in process_files function: {e}'
        logging.error(err_msg)
        raise Exception(err_msg)


def main(input_dir, output_dir):
    start = time.time()

    # Set Log Level
    logging.basicConfig(level = logging.INFO)
    logging.info(f"Called with input dir: {input_dir}")

    if not os.path.exists(input_dir):
        logging.warning("Input Path doesn't exists")
        return "Input Path doesn't exists"

    files = os.listdir(input_dir)
    if not files:
        logging.warning("No files found for processing")
        return "No files found for processing"

    # Create output Directory if not exists
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    df_list = []
    for file in files:
        # # For creating each Parquet file for each csv file
        # if file.endswith(".csv"):
        #     file_path = os.path.join(input_dir, file)
        #     logging.info(f"Processing input CSV file {file} - Started")
        #     df = read_csv(file_path)
        #     if not process_files(df, os.path.join(output_dir, os.path.basename(file)[:-4] + ".parquet")):
        #         raise Exception("Error in Processing CSV Files")
        #     logging.info(f"Processing input CSV file {file} - Completed")

        # If we need to create one Parquet output file
        if file.endswith(".csv"):
            file_path = os.path.join(input_dir, file)
            logging.info(f"Reading input CSV file {file_path} - Started")
            df = read_csv(file_path)
            df_list.append(df)
            logging.info(f"Reading input CSV file {file_path} - Completed")

    # If we need to create one Parquet output file
    if df_list:
        merged_df = merge_dfs(df_list)
        logging.info(f"Processing CSV Files - Started")
        if not process_files(merged_df, os.path.join(output_dir, "whether.parquet")):
            raise Exception("Error in Processing CSV Files")
        logging.info(f"Processing CSV Files - Completed")

    return "Successfully Processed All Files"


if __name__ == '__main__':
    status = main(input_csv_dir_path, output_parquet_dir_path)
    logging.info(status)
