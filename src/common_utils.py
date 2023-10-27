# 
import psycopg2
import os
import io
import pandas as pd
from sqlalchemy import create_engine
import glob
import os
from functools import wraps
import time
import datetime 
from collections import defaultdict


def timing_decorator(func):
    def wrapper(*args, **kwargs):
        start_time_epoch = time.time()
        start_time=datetime.datetime.fromtimestamp(start_time_epoch).strftime("%Y-%m-%d %H:%M:%S")
        result = func(*args, **kwargs)
        end_time_epoch = time.time()
        end_time = datetime.datetime.fromtimestamp(end_time_epoch).strftime("%Y-%m-%d %H:%M:%S")
        duration = end_time_epoch - start_time_epoch
        print(f"Function '{func.__name__}' started at {start_time}.")
        print(f"Function '{func.__name__}' ended at {end_time}.")
        print(f"Total duration: {duration:.4f} seconds")
        return result
    return wrapper

def db_postgresql_login():
    user='postgres'
    password='password'
    host='localhost'
    connection_string='postgresql://{}:{}@{}:5432/corteva'.format(user,password,host)
    engine = create_engine(connection_string)
    return engine

def execute_postgress_db(query=None,sql_parameters=None):
    db_credentials = {
    "user": "postgres",
    "password": "password",
    "host": "localhost",
    "port": "5432",
    "database": "corteva",
}
    # Connect to the PostgreSQL database
    connection = psycopg2.connect(**db_credentials)
    # Create a cursor object
    cursor = connection.cursor()
    return cursor.execute(query,sql_parameters)