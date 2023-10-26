import psycopg2
import os
import io
from IPython.display import display
import pandas as pd
from sqlalchemy import create_engine
import glob
import os
from functools import wraps
import time
import datetime 

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

def db_postgresql_login():
    user='postgres'
    password='password'
    host='localhost'
    connection_string='postgresql://{}:{}@{}:5432/corteva'.format(user,password,host)
    engine = create_engine(connection_string)
    return engine

@timing_decorator
def write_weather_data(sql_engine):
    #read text files 
    path = 'C:/Users/Bhargava Reddy/Downloads/corteva/code-challenge-template/wx_data/'
    all_files = glob.glob(os.path.join(path , "*.txt"))
    final_dataframe = []
    print("Looping through text files and load to pandas dataframe")
    for filename in all_files:
        read_textfile_df = pd.read_csv(filename, delimiter="\t", names=['measurement_date', 'max_temperature', 'min_temperature','precipitation'])
        read_textfile_df['weather_station_id'] = os.path.basename(filename.rsplit( ".", 1 )[ 0 ] )
        final_dataframe.append(read_textfile_df)
    df = pd.concat(final_dataframe, ignore_index=True)
    df['measurement_date']=pd.to_datetime(df['measurement_date'], format='%Y%m%d').dt.strftime('%Y-%m-%d')
    df=df.reindex(['weather_station_id','measurement_date', 'max_temperature', 'min_temperature','precipitation'], axis=1)

    print(f"count of final dataframe..{len(df)}")
    df.dtypes
    #Wiritng to temp table for insert and updates to final table
    from sqlalchemy import create_engine
    engine = create_engine('postgresql://postgres:password@localhost/corteva')

    df.to_sql('weather_temp_table_t', con=engine, if_exists='replace')
       
    # Insert and update the values into the PostgreSQL table
    upsert_sql = """
                    INSERT INTO WEATHER_DATA_PARTITION 
                    select t_wsi.weather_station_id,cast(t_wsi.measurement_date as DATE), t_wsi.max_temperature,t_wsi.min_temperature,t_wsi.precipitation 
                        from weather_temp_table_t t_wsi
                    ON CONFLICT (measurement_date,weather_station_id)
                    DO UPDATE
                    SET max_temperature = EXCLUDED.max_temperature, min_temperature = EXCLUDED.min_temperature,precipitation = EXCLUDED.precipitation;
                """
    records_to_insert=()
    cursor.execute(upsert_sql,records_to_insert)
    
    
    return df
                     
#Writing raw data from text files to postgress table
#Function load_data_to_postgress(<function db_postgresql_login at 0x000002126CF388B0>,) {} Took 260.2584 seconds
#RecordCounter Rows: 1729958
#Function 'load_data_to_postgress' started at 2023-10-25 15:40:14.
#Function 'load_data_to_postgress' ended at 2023-10-25 15:44:16.
#Total duration: 241.1412 seconds
@timing_decorator
def load_data_to_postgress(sql_engine):
   
    try:
        # Open the text file for reading
        path = 'C:/Users/Bhargava Reddy/Downloads/corteva/code-challenge-template/wx_data'
        all_files = glob.glob(os.path.join(path , "*.txt"))
        RecordCounter = 0
        for filename in all_files:
            print(f"filename...{filename}")
            with open(filename, 'r') as file:
                for line in file:
                    # Split each line into values (assuming a comma-separated format)
                    values = line.strip().split('\t')
                    weather_station_id=os.path.basename(filename.rsplit( ".", 1 )[ 0 ] )
 
                    # Insert the values into the PostgreSQL table
                    upsert_sql = """
                                    INSERT INTO WEATHER_DATA (weather_station_id, measurement_date, max_temperature,min_temperature,precipitation)
                                    VALUES (%s, %s, %s, %s, %s)
                                    ON CONFLICT (measurement_date,weather_station_id)
                                    DO UPDATE
                                    SET max_temperature = EXCLUDED.max_temperature, min_temperature = EXCLUDED.min_temperature,precipitation = EXCLUDED.precipitation;
                                """
                    cursor.execute(upsert_sql, (weather_station_id,values[0], values[1],values[2], values[3]))
                    inserted_rows = cursor.rowcount
                    RecordCounter = inserted_rows + RecordCounter
        # Get the number of rows inserted
        print(f"RecordCounter Rows: {RecordCounter}")
        #commit 
        connection.commit()
        
    except (Exception, psycopg2.Error) as error:
        print(f"Error: {error}")
        connection.rollback()  # Rollback the transaction if there's an error

    finally:
        if connection:
            cursor.close()
            connection.close()
            print("Connection closed.")
    
#RecordCounter Rows: 1729958
#Connection closed.
#Function 'load_data_to_postgress_partition' started at 2023-10-25 15:47:13.
#Function 'load_data_to_postgress_partition' ended at 2023-10-25 15:51:29.
#Total duration: 256.1821 seconds
#None
@timing_decorator
def load_data_to_postgress_partition(sql_engine):
   
    try:
        # Open the text file for reading
        path = 'C:/Users/Bhargava Reddy/Downloads/corteva/code-challenge-template/wx_data'
        all_files = glob.glob(os.path.join(path , "*.txt"))
        RecordCounter = -1
        for filename in all_files:
            print(f"filename...{filename}")
            with open(filename, 'r') as file:
                for line in file:
                    # Split each line into values (assuming a comma-separated format)
                    values = line.strip().split('\t')
                    weather_station_id=os.path.basename(filename.rsplit( ".", 1 )[ 0 ] )
 
                    # Insert the values into the PostgreSQL table
                    upsert_sql = """
                                    INSERT INTO WEATHER_DATA_PARTITION (weather_station_id, measurement_date, max_temperature,min_temperature,precipitation)
                                    VALUES (%s, %s, %s, %s, %s)
                                    ON CONFLICT (measurement_date,weather_station_id)
                                    DO UPDATE
                                    SET max_temperature = EXCLUDED.max_temperature, min_temperature = EXCLUDED.min_temperature,precipitation = EXCLUDED.precipitation;
                                """
                    cursor.execute(upsert_sql, (weather_station_id,values[0], values[1],values[2], values[3]))
                    inserted_rows = cursor.rowcount
                    RecordCounter = RecordCounter + inserted_rows 
        # Get the number of rows inserted
        print(f"RecordCounter Rows: {RecordCounter}")
        #commit 
        connection.commit()
        
    except (Exception, psycopg2.Error) as error:
        print(f"Error: {error}")
        connection.rollback()  # Rollback the transaction if there's an error

    finally:
        if connection:
            cursor.close()
            connection.close()
            print("Connection closed.")   

if __name__ == "__main__":
    print("invoking the fucntion")
    sql_engine=db_postgresql_login    
    df_data=write_weather_data(sql_engine)
    #data=load_data_to_postgress(sql_engine)
    #data=load_data_to_postgress_partition(sql_engine)
    
    print(df_data)
    