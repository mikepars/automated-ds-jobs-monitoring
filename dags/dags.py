'''
=================================

.___        __                    .___             __  .__               
|   | _____/  |________  ____   __| _/_ __   _____/  |_|__| ____   ____  
|   |/    \   __\_  __ \/  _ \ / __ |  |  \_/ ___\   __\  |/  _ \ /    \ 
|   |   |  \  |  |  | \(  <_> ) /_/ |  |  /\  \___|  | |  (  <_> )   |  \ 
|___|___|  /__|  |__|   \____/\____ |____/  \___  >__| |__|\____/|___|  /
         \/                        \/           \/                    \/ 

Information
    Name  : Michael Parsaoran
    Batch : HCK-017

Objective
    This python script is used to extract data from PostGres, then transforms the data so that we can use it for data analysis, 
    and then load the data into elasticsearch.

Dataset Information : The dataset contains all job posting in Glassdoor.
=================================
'''

# import libraries
import datetime as dt
import numpy as np
import pandas as pd

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

import pandas as pd
from elasticsearch import helpers, Elasticsearch
from elasticsearch_dsl import connections
from sqlalchemy import create_engine # connection to postgres

# global variables definition
db_name = 'airflow'
username = 'airflow'
password = 'airflow'
host = 'postgres'

# establish connection
postgres_url= f'postgresql+psycopg2://{username}:{password}@{host}/{db_name}'
engine = create_engine(postgres_url)
connection = engine.connect()

# file paths
source_data = 'C:\Code\DE\p2-ftds017-hck-m3-mikepars\dags\P2M3_michael_source_data.csv' # local file that will be sent to postgres
raw_data = 'C:\Code\DE\p2-ftds017-hck-m3-mikepars\dags\P2M3_michael_raw_data.csv'
cleaned_data = 'C:\Code\DE\p2-ftds017-hck-m3-mikepars\dags\P2M3_michael_cleaned_data.csv'

# Elasticsearch url
es_client = connections.create_connection(hosts=['http://localhost:9200/'])

default_args = {
    'owner': 'michael',
    'start_date': dt.datetime(2024, 7, 22)
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}

def csv_to_postgres():
    '''
    this function is made to read a CSV file, then create a table in postgres server
    defined by arguments received.
    '''
    print(connection)
    # load csv from path
    df = pd.read_csv(source_data) # local file that will be sent to postgres

    # create a table from df in the connected server
    df.to_sql(name='table_m3', con=connection, if_exists='replace', index=False)
    print(connection)

def postgres_to_csv():
    '''
    this function is used to pull a specific table (in this instance we pull table_m3)
    then save it to CSV format.
    '''

    # load table from connected server
    df = pd.read_sql_query('select * from table_m3', connection)

    # save loaded table into csv format
    df.to_csv(raw_data, sep=',', index=False)

def clean_raw_data():
    df = pd.read_csv(raw_data)

    # Creating new dataset for output
    df_clean = pd.DataFrame()

    # Dropping duplicates and missing
    df.drop_duplicates(inplace=True)
    df.dropna(inplace=True)

    # Creating new dataset for output
    df_clean = pd.DataFrame()

    # Salary information
    df_clean['salary_estimate'] = df['Salary Estimate'].apply(lambda x: x.split('(')[0]).apply(lambda x: x.replace('K','').replace('$','')).apply(lambda x: x.replace(' ', ''))
    df_clean['min_salary'] = df_clean['salary_estimate'].apply(lambda x: int(x.split('-')[0]))
    df_clean['max_salary'] = df_clean['salary_estimate'].apply(lambda x: int(x.split('-')[1]))
    df_clean['med_salary'] = df_clean['max_salary'] - df_clean['min_salary']
    df_clean['range_salary'] = ((df_clean['max_salary'] + df_clean['min_salary']) / 2).astype(int)
    df_clean.drop(columns = 'salary_estimate', inplace = True)

    # Company information
    df_clean['company_name'] = df['Company Name'].apply(lambda x: x.split('\n')[0])
    df_clean['rating'] = df['Rating']
    df_clean['rating'] = np.where(df_clean['rating'] == -1.0, 0.0, df_clean['rating'])
    df_clean['location'] = df['Location']
    df_clean['headquarters'] = df['Headquarters']
    df_clean['headquarters'] = np.where(df_clean['headquarters'] == '-1', df_clean['location'], df_clean['headquarters'])

    # Skillset information
    df_clean['jobdesc'] = df['Job Description']
    skills =  ['python', 'java', 'c++', 'javascript', 'tensorflow', 'pytorch', 'computer vision', 'spark', 'mlops', 'ci/cd', 
            'data cleaning', 'data manipulation', 'etl', 'elt', 'tableau', 'power bi', 'big data', 'docker', 'kubernetes', 
            'hadoop', 'kafka', 'aws', 'google cloud', 'azure', 'mongodb', 'kibana', 'elasticsearch', 'airflow', 'scala', 
            'go', 'rust', r'\bR\b', 'dashboard', 'google looker', 'c#', r'\bC\b']

    jd = df['Job Description']

    techtools = []

    for jobdesc in jd:
        lst = []
        for skill in skills:
            if skill in jobdesc.lower():
                lst.append(skill)
        techtools.append(lst)

    for lst in techtools:
        if lst == []:
            lst.append("None")

    df_clean['techtools'] = techtools
    df_clean.drop(columns = 'jobdesc', inplace = True)

    df = df_clean.copy()
    df.to_csv(cleaned_data, sep=',', index=False)

def upload_to_elasticsearch():
    df = df.read_csv(cleaned_data)
    def doc_generator(df):
        df_iter = df.iterrows()
        for index, document in df_iter:
            yield {
                "_index" : 'P2M3_michael_cleaned_dataset',
                "_type" : '_doc',
                "_source" : document
            }

    helpers.bulk(es_client, doc_generator(df))

# define the DAG with a specific schedule and description.
with DAG('Automate_cleaning', 
        default_args=default_args,
        description='Automate_cleaning',
        schedule_interval='30 6 * * *'  # Schedule to run every day at 06:30 
        ) as dag:
    
    # load raw data into PostgreSQL using a Python function
    load_raw = PythonOperator(task_id='load_raw_data', 
                              python_callable = csv_to_postgres)
    
    # fetch raw data from PostgreSQL and save it to a CSV file using a Python function.
    export_raw = PythonOperator(task_id='export_raw_data', 
                               python_callable = postgres_to_csv)
    
    # clean raw data and save it to a CSV file using a Python function.
    clean_data = PythonOperator(task_id='clean_raw_data', 
                               python_callable = clean_raw_data)
    
    # upload clean data to elasticsearch using a Python function.
    upload_data = PythonOperator(task_id='upload_to_elasticsearch', 
                               python_callable = upload_to_elasticsearch)
    
# define the task dependencies
load_raw >> export_raw >> clean_data >> upload_data