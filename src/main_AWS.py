"""
Only needed for testing locally
from pathlib import Path
import os
"""
import json
from datetime import datetime
import pandas as pd
from urllib import request
import boto3


def handler(event, context):
    url, csv_file, s3_bucket_file_path = load_paths('paths.json')
    page = load_url(url)
    new_cases = load_and_clean_new_cases(page)
    df = read_csv_from_s3_bucket(s3_bucket_file_path)
    df_new = append_new_cases_to_s3_csv(df, new_cases)
    return_csv_to_s3_bucket(df_new, s3_bucket_file_path)


def load_url_s3_bucket(json_path_file):
    """Loads the json file containing the url and bucket details.
    Returns the loaded items."""
    with open(json_path_file) as j:
        data = json.load(j)
    url = data['url']
    csv_file = data['csv_file']
    bucket = data['s3_bucket']
    file = data['filename']
    return url, csv_file, s3_bucket_file_path


def load_url(url):
    """Makes the url request to RKI website and returns the loaded html."""
    HEADERS = {'User-Agent':
               'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36\
               (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.3'}
    req = request.Request(url, headers=HEADERS)
    with request.urlopen(req) as p:
        page = p.read()
    return page


def load_and_clean_new_cases(page):
    """Loads up the tables from the html page, cleans them and returns the
    cleaned array of new Covid-19 cases in Germany."""
    dfs = pd.read_html(page, header=1, encoding='utf-8', thousands='.',
                       decimal=',')
    new_name = ['region', 'number', 'new', 'per 100k residents', 'deaths']
    dfs[0].columns = new_name
    new_df = dfs[0].drop(['number', 'per 100k residents', 'deaths'], axis=1)
    new_df = new_df.drop(dfs[0].index[16], axis=0)
    cases = new_df['new']
    new_cases = [int(c) if (c % 1 == 0) else int(c*1000) for c in cases]
    return new_cases


def read_csv_from_s3_bucket(s3_bucket_file_path):
    """Fetches the csv file from s3 bucket for appending the new cases. Returns
    csv as a df."""
    s3 = boto3.client('s3')
    df = pd.read_csv(s3_bucket_file_path)
    return df


def append_new_cases_to_s3_csv(df, new_cases):
    """Adds new column onto df with the latest cases. Returns the df."""
    df[datetime.now()] = new_cases
    return df_new


def return_csv_to_s3_bucket(df_new, s3_bucket_file_path):
    """Re-saves the csv file to s3 bucket. Returns nothing."""
    df.to_csv(s3_bucket_file_path, index=False)
    return
