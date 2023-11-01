# pylint: disable=import-error
"""Module loads text files from windows directory to postgress tables and do aggregation on wetaher staton id and year 
    python files need text file locations and postgress tables name
"""

import glob
import os
import psycopg2
from common_utils import timing_decorator

# Load variables..
PATH = 'C:/Users/Bhargava Reddy Yeddu/Downloads/corteva/code-challenge-template/wx_data'
TABLE_NAME="WEATHER_DATA"
# TABLE_NAME = "WEATHER_DATA_PARTITION"
AGG_TABLE_NAME = "WEATHER_AGGREGATE_DATA"

print(f"processing files in folder: {PATH}")
# connecting to postgress database
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
# def write_weather_data():
#     #read text files
#     all_files = glob.glob(os.path.join(path , "*.txt"))
#     final_dataframe = []
#     print("Looping through text files and load to pandas dataframe")
#     for filename in all_files:
#         read_textfile_df = pd.read_csv(filename, delimiter="\t", names=['measurement_date', 'max_temperature', 'min_temperature','precipitation'])
#         read_textfile_df['weather_station_id'] = os.path.basename(filename.rsplit( ".", 1 )[ 0 ] )
#         final_dataframe.append(read_textfile_df)
#     df = pd.concat(final_dataframe, ignore_index=True)
#     df['measurement_date']=pd.to_datetime(df['measurement_date'], format='%Y%m%d').dt.strftime('%Y-%m-%d')
#     df=df.reindex(['weather_station_id','measurement_date', 'max_temperature', 'min_temperature','precipitation'], axis=1)
#     print(f"count of final dataframe..{len(df)}")
#     df.dtypes
#     #Wiritng to temp table for insert and updates to final table
#     from sqlalchemy import create_engine
#     engine = create_engine('postgresql://postgres:password@localhost/corteva')
#     df.to_sql('weather_temp_table_t', con=engine, if_exists='replace')
#     # Insert and update the values into the PostgreSQL table
#     upsert_sql = """
#                     INSERT INTO WEATHER_DATA_PARTITION
#                     select t_wsi.weather_station_id,cast(t_wsi.measurement_date as DATE), t_wsi.max_temperature,t_wsi.min_temperature,t_wsi.precipitation
#                         from weather_temp_table_t t_wsi
#                     ON CONFLICT (weather_station_id,measurement_date)
#                     DO UPDATE
#                     SET max_temperature = EXCLUDED.max_temperature, min_temperature = EXCLUDED.min_temperature,precipitation = EXCLUDED.precipitation;
#                 """
#     records_to_insert=()
#     cursor.execute(upsert_sql,records_to_insert)
#     return df
# Writing raw data from text files to postgress table
# Function load_data_to_postgress(<function db_postgresql_login at 0x000002126CF388B0>,) {} Took 260.2584 seconds
# RecordCounter Rows: 1729958
# Function 'load_data_to_postgress' started at 2023-10-25 15:40:14.
# Function 'load_data_to_postgress' ended at 2023-10-25 15:44:16.
# Total duration: 241.1412 seconds
@timing_decorator
def load_data_to_postgress():
    "Fucntion loop through file in a directory and load to postgress tables"
    try:
        all_files = glob.glob(os.path.join(PATH, "*.txt"))
        recordcounter = 0
        for filename in all_files:
            print(f"filename...{filename}")
            with open(filename, 'r', encoding="utf-8") as file:
                for line in file:
                    # Split each line into values (assuming a comma-separated format)
                    values = line.strip().split('\t')
                    weather_station_id = os.path.basename(
                        filename.rsplit(".", 1)[0])
                    # Insert the values into the PostgreSQL table
                    
                    upsert_sql = f"""INSERT INTO {TABLE_NAME} (weather_station_id, measurement_date, max_temperature,min_temperature,precipitation)
                                    VALUES (%s, %s, %s, %s, %s)
                                    ON CONFLICT (measurement_date,weather_station_id)
                                    DO UPDATE
                                    SET max_temperature = EXCLUDED.max_temperature, min_temperature = EXCLUDED.min_temperature,precipitation = EXCLUDED.precipitation;
                                """
                    cursor.execute(upsert_sql, (weather_station_id,
                                    values[0], values[1], values[2], values[3]))
                    inserted_rows = cursor.rowcount
                    recordcounter = inserted_rows + recordcounter
        # Get the number of rows inserted
        print(f"RecordCounter Rows: {recordcounter}")
        # commit
        connection.commit()
    except (Exception, psycopg2.Error) as error:
        print(f"Error: {error}")
        connection.rollback()  # Rollback the transaction if there's an error
     
@timing_decorator
def aggregate_data():
    "Function aggregate weather data and load to postgress tables"
    # unit conversion
    # average temperatures are divided by 10 to convert from "in tenth degree Celsius" to "degree Celsius" and
    # total precipitation is divided by 100 to convert from tenth of "Millimeters" to "Centimeters", 1mm = 0.1cm, 0.1mm = 0.01cm
    try:
        agg_query = f"""INSERT INTO {AGG_TABLE_NAME} 
                        (weather_station_id, measurement_year, avg_max_temperature,avg_min_temperature,total_precipitation_cm)
                            WITH aggregate_table AS (
                                with total_precipitation as (select weather_station_id, DATE_PART('year',measurement_date) as measurement_year,sum(precipitation)/100 as total_precipitation_cm 
                                                            from public.weather_data 
                                                            where ( precipitation != -9999)
                                                            GROUP BY weather_station_id,DATE_PART('year',measurement_date)),
                                    avg_max_temperature as (select weather_station_id, DATE_PART('year',measurement_date) as measurement_year,cast(avg(max_temperature)/10 as decimal(18,3)) as avg_max_temperature 
                                                            from public.weather_data 
                                                            where (max_temperature != -9999 )
                                                            GROUP BY weather_station_id,DATE_PART('year',measurement_date)),
                                    avg_min_temperature as (select weather_station_id, DATE_PART('year',measurement_date) as measurement_year,cast(avg(min_temperature)/10 as decimal(18,3)) as avg_min_temperature
                                                            from public.weather_data 
                                                            where (min_temperature != -9999)
                                                            GROUP BY weather_station_id,DATE_PART('year',measurement_date))
                                                            
                                select tpp.weather_station_id,tpp.measurement_year,
                                        tpp.total_precipitation_cm,max_temp.avg_max_temperature,min_temp.avg_min_temperature
                                from  total_precipitation tpp
                                INNER JOIN avg_max_temperature max_temp
                                ON tpp.weather_station_id = max_temp.weather_station_id and tpp.measurement_year = max_temp.measurement_year 
                                INNER JOIN avg_min_temperature min_temp
                                ON tpp.weather_station_id = min_temp.weather_station_id and tpp.measurement_year = min_temp.measurement_year
                            )
                            SELECT
                                weather_station_id,
                                measurement_year,
                                avg_max_temperature,
                                avg_min_temperature,
                                total_precipitation_cm
                            FROM aggregate_table
                        ON CONFLICT (measurement_year,weather_station_id)
                                DO UPDATE
                                SET avg_max_temperature = EXCLUDED.avg_max_temperature, 
                                    avg_min_temperature = EXCLUDED.avg_min_temperature,
                                    total_precipitation_cm = EXCLUDED.total_precipitation_cm;"""
        cursor.execute(agg_query)
        inserted_rows = cursor.rowcount
        print(
            f"Number of rows insered to aggregate table-----> {inserted_rows}")
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
    # load_data_to_postgress()
    print("executing aggregate fucntion")
    aggregate_data()
