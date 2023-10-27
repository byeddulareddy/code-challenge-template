# 
import glob
import os
import psycopg2
from common_utils import  timing_decorator
import sqlite3

# Load variables..
PATH = 'C:/Users/Bhargava Reddy Yeddu/Downloads/corteva/code-challenge-template/wx_data'
# TABLE_NAME="WEATHER_DATA"
TABLE_NAME = "WEATHER_DATA_PARTITION"
AGG_TABLE_NAME = "WEATHER_AGGREGATE_DATA"
#connecting to postgress database
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

@timing_decorator
def postvalidation():
    """Function to check records which present in text files with postgress tables."""
    all_files = glob.glob(os.path.join(PATH , "*.txt"))
    text_file_count = 0
    for filename in all_files:
        with open(filename, 'r' , encoding="utf-8") as file:
                for line in file:
                    if line != "\t":
                        text_file_count += 1
    print('Number of rows in text file ', text_file_count)
    cursor.execute(f"""select count(*) from {TABLE_NAME} """)
    result=cursor.fetchone()
    table_count = result[0]
    print('Number of rows in table ', table_count)
    if text_file_count!=table_count:
        print("Validation failed with count mismatch checking rows which is not present in table")
        for filename in all_files:
            with open(filename, 'r', encoding="utf-8") as file:
                    for line in file:
                        # Split each line into values (assuming a comma-separated format)
                        values = line.strip().split('\t')
                        weather_station_id=os.path.basename(filename.rsplit( ".", 1 )[ 0 ] )
                        query_check = f"""   select * from {TABLE_NAME}
                                            where
                                              weather_station_id='{weather_station_id} '
                                              and
                                              TO_CHAR(measurement_date,'YYYYMMDD')='{values[0]}'
                                       """
                        cursor.execute(query_check)
                        result = cursor.fetchone()
                        if result:
                            print("Row exists in the table")
                        else:
                            print("Row does not exist in the table")
                    


if __name__ == "__main__":
    print("invoking the fucntion")
    postvalidation()