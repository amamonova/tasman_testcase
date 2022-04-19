"""
Script for downloading data from https://www.usajobs.gov
"""


import argparse
from collections import OrderedDict
import json
import sqlite3
import smtplib
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart

import requests
from os import path, listdir
from typing import List
from urllib.parse import urlencode
import logging

import pandas as pd

from config import AUTHORIZATION_EMAIL, AUTHORIZATION_KEY, SERVICE_EMAIL


BASE_URL = "https://data.usajobs.gov/api/"
PAGE_LIMIT = 500
COLS = OrderedDict({
    'salary_max': 'float',
    'salary_min': 'float',
    'organization_name': 'text',
    'position_title': 'text NOT NULL',
    'publication_date': 'date',
    'salary_interval': 'text',
    'who_may_apply': 'text'
    })
DB_NAME = 'tasman_db'
REPORTS_PATH = './reports'
POSITIONS_TABLE_NAME = 'positions'


def db_connect(db_name: str):
    """Connects to database and returns a database connection object. """
    try:
        conn = sqlite3.connect(db_name)
        return conn
    except Exception as ex:
        logging.error(f'db_connect function: {ex}')
        raise ex


def calculate_days_delta(db_name: str, table_name: str):
    """
    Returns how many days should be downloaded.
    It is necessary for not downloading duplicates from API,
    when script running by cron process with same keywords and positions.
    It calculates days delta between current date and maximum publication date from DB.
    """
    days_delta = None
    with db_connect(db_name) as conn:
        check_table_exist_query = f"""SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"""
        table_len = len(conn.execute(check_table_exist_query).fetchall())
        if table_len:
            query = f"""select max(publication_date) from {table_name}"""
            max_date = conn.execute(query).fetchall()[0][0]
            if max_date is None:
                return days_delta
            days_delta = pd.to_datetime('today') - pd.to_datetime(max_date)
    if days_delta:
        return days_delta.days
    return None


def get_api_call(endpoint: str, params: dict, base_url: str = BASE_URL, page_limit: int = PAGE_LIMIT):
    """
    Makes a GET request with appropriate parameters, authentication,
    while respecting page and rate limits, and paginating if needed. 
    
    Returns a JSON API response object. 
    """

    headers = {'Host': 'data.usajobs.gov',
               'User-Agent': f'{AUTHORIZATION_EMAIL}',
               'Authorization-Key': f'{AUTHORIZATION_KEY}'}
    url = f'{base_url}{endpoint}?{urlencode(params)}&ResultsPerPage={page_limit}'
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return json.loads(response.text)
    raise Exception("Something wrong with getting data from API")


def extract_positions(titles: List[str], keywords: List[str], days=None):
    """
    Makes API calls for titles and keywords, parses the responses. 
    
    Returns the values ready to be loaded into database.
    """
    endpoint = 'search'
    params = {'Keyword': ','.join(keywords),
              'PositionTitle': ','.join(titles),
              'DatePosted': days}

    if days is None:
        params.pop('DatePosted')
    json_response = get_api_call(endpoint, params)
    parsed_data = parse_positions(json_response)

    return [list(item.values()) for item in parsed_data]


def parse_positions(response_json: List[dict]):
    """
    Parses a response JSON for wanted fields. 

    Returns a list of positions of appropriate object type. 
    """

    keys = ['PositionTitle', 'OrganizationName', 'PublicationStartDate']
    result = []

    for item in response_json['SearchResult']['SearchResultItems']:
        item = item['MatchedObjectDescriptor']
        new_item = {key: (item[key].lower() if key != 'PublicationStartDate' else item[key]) for key in keys}
        new_item['WhoMayApply'] = item['UserArea']['Details']['WhoMayApply']['Name']
        new_item['MinimumRange'] = float(item['PositionRemuneration'][0]['MinimumRange'])
        new_item['MaximumRange'] = float(item['PositionRemuneration'][0]['MaximumRange'])
        new_item['RateIntervalCode'] = item['PositionRemuneration'][0]['RateIntervalCode']
        result.append(OrderedDict(sorted(new_item.items())))
    return result


def prep_database(db_name: str):
    """Connects to database and creates tables if necessary. """
    cols_dtypes = ", ".join([f"{x} {y}" for x, y in COLS.items()])
    query = f""" CREATE TABLE IF NOT EXISTS positions ({cols_dtypes}); """
    conn = db_connect(db_name)
    try:
        cursor = conn.cursor()
        cursor.execute(query)
    except Exception as ex:
        logging.error(f'prep_database function: {ex}')


def load_data(row_values: List[dict], db_name: str, table_name: str):
    """Connects to database and loads values in corresponding tables. """
    query = ""
    if table_name == "positions":
        cols_names = ", ".join(COLS.keys())
        values_template = ",?" * len(COLS.keys())
        values_template = values_template[1:]
        query = f"""INSERT INTO {table_name}({cols_names})
                    VALUES({values_template})"""
    with db_connect(db_name) as conn:
        conn.execute(query, row_values)


def export_to_csv(db_name: str, output_path: str, filename: str, query: str):
    """
    Run SQL query and export it to csv
    """
    file_path = path.join(output_path, filename)
    with db_connect(db_name) as conn:
        db_df = pd.read_sql_query(query, conn)
        db_df.to_csv(file_path, index=False)


def run_analysis(db_name: str, output_path: str):
    """
    Runs 3 SQL queries to obtain results that could answer the following questions:
    1. How do *monthly* starting salaries differ across positions with different titles and keywords?
    2. Do (filtered) positions for which 'United States Citizens' can apply have a higher average salary than those
       that 'Student/Internship Program Eligibles' can apply for? (by month)
    3. What are the organisations that have most open (filtered) positions?
    
    Exports results of queries into CSV files in the `output_path` directory.

    ** Feel free to break this function down into smaller units 
    (hint: potentially have a `export_csv(query_result)` function)  
    """
    queries = ["""
        select 
        position_title, 
        strftime("%m-%Y", publication_date) as month, 
        sum(case 
        when salary_interval = "Per Year" then salary_min / 12 
        when salary_interval = "Bi-weekly" then salary_min * 2.17
        when salary_interval = "Per Month" then salary_min
        end) as salary_min
        from positions 
        where salary_interval in ("Per Year", "Bi-weekly", "Per Month")
        group by position_title, month
        order by salary_min
        """,
        """
        select who_may_apply, avg((salary_max-salary_min)/2) as avg_salary
        from positions
        where who_may_apply in ("United States Citizens", "Student/Internship Program Eligibles")
        group by who_may_apply
        """,
        """
        select organization_name, count(position_title) as positions_per_organization
        from positions
        order by positions_per_organization
        """]

    for idx, query in enumerate(queries):
        export_to_csv(db_name, output_path, f"analysis_{idx}.csv", query)


def send_reports(recipient_email: str, reports_path: str):
    """
    Loops through present CSV files in reports_path, 
    and sends them via email to recipient. 

    Returns None
    """
    files_in_dir = [path.join(reports_path, f) for f in listdir(reports_path) if path.isfile(path.join(reports_path, f))]
    for idx, report in enumerate(files_in_dir):
        msg = MIMEMultipart()
        with open(report, 'r') as fp:
            attachment = MIMEBase('multipart', 'csv')
            attachment.set_payload(fp.read())
        msg.attach(attachment)
        msg['Subject'] = f'The contents of report_{idx}'
        msg['To'] = recipient_email
        msg['From'] = SERVICE_EMAIL

        with smtplib.SMTP("localhost", 1000) as server:
            server.send_message(msg)


def download_data(db_name: str, positions_table_name: str, positions: List[str], keywords: List[str]):
    """
    Process loading data to DB
    """
    prep_database(db_name)
    days = calculate_days_delta(db_name, positions_table_name)
    data = extract_positions(positions, keywords, days)
    for row in data:
        load_data(row, db_name, positions_table_name)
    logging.info("Data downloaded")


def process_send_reports(db_name: str, recipient_email: str, reports_path: str):
    """
    Run analysis and send report via email
    """
    run_analysis(db_name, reports_path)
    send_reports(recipient_email, reports_path)
    logging.info("Reports sent")


if __name__ == "__main__":
    """
    Puts it all together, and runs everything end-to-end. 

    Feel free to create additional functions that represent distinct functional units, 
    rather than putting it all in here. 

    Optionally, enable running this script as a CLI tool with arguments for position titles and keywords. 
    """
    parser = argparse.ArgumentParser(description='Process downloading and analysing jobs data.')
    parser.add_argument('--postions_title', nargs="*", default=['Data Analyst', 'Data Scientist', 'Data Engineer'])
    parser.add_argument('--keywords', nargs="*", default=['data', 'analysis', 'analytics'])
    parser.add_argument('--recipient_email', default='test@example.com')

    args = parser.parse_args()

    download_data(DB_NAME, POSITIONS_TABLE_NAME, args.postions_title, args.keywords)
    process_send_reports(DB_NAME, args.recipient_email, REPORTS_PATH)
