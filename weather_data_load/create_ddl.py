"""Module to create postgress tables"""
import psycopg2

LOCALHOST='localhost'

#Establishing the connection
conn = psycopg2.connect(
   database="corteva", user='postgres', password='password', host=LOCALHOST, port= '5432'
)
#Creating a cursor object using the cursor() method
cursor = conn.cursor()

# Creating table with primary key 
#Doping WEATHER_DATA table if already exists.
# cursor.execute("DROP TABLE IF EXISTS WEATHER_DATA")

#Creating table as per requirement with no partitions
CREATE_WEATHER_DATA_TABLE ='''CREATE TABLE IF NOT EXISTS WEATHER_DATA(
                                                   weather_station_id VARCHAR(100) NOT NULL,
                                                   measurement_date DATE NOT NULL,
                                                   max_temperature INTEGER,
                                                   min_temperature INTEGER,
                                                   precipitation INTEGER,
                                                  primary key (weather_station_id, measurement_date) 
                                                );
                           '''
cursor.execute(CREATE_WEATHER_DATA_TABLE)

#Creating weather aggregate table 
CREATE_WEATHER_AGGREGATE_DATA_TABLE ='''CREATE TABLE IF NOT EXISTS WEATHER_AGGREGATE_DATA(
                                                   weather_station_id VARCHAR(100) NOT NULL,
                                                   measurement_year INTEGER NOT NULL,
                                                   avg_max_temperature DOUBLE PRECISION,
                                                   avg_min_temperature DOUBLE PRECISION,
                                                   total_precipitation_cm DOUBLE PRECISION,
                                                  primary key (weather_station_id, measurement_year) 
                                                );
                           '''
cursor.execute(CREATE_WEATHER_AGGREGATE_DATA_TABLE)


# Creating table with primary key and range partition
#Doping WEATHER_DATA_PARTITION table if already exists.
# cursor.execute("DROP TABLE IF EXISTS WEATHER_DATA_PARTITION")
CREATE_WEATHER_DATA_PARTITION_TABLE ='''CREATE TABLE IF NOT EXISTS WEATHER_DATA_PARTITION(
                                                   weather_station_id VARCHAR(100),
                                                   measurement_date DATE NOT NULL,
                                                   max_temperature INTEGER ,
                                                   min_temperature INTEGER,
                                                   precipitation INTEGER,
                                                   primary key (weather_station_id, measurement_date) 
                                                )
                              PARTITION BY RANGE (measurement_date);
                           '''
cursor.execute(CREATE_WEATHER_DATA_PARTITION_TABLE)

PARTITION_QUERY_LIST=[""" CREATE TABLE IF NOT EXISTS measurement_date_1985_to_1995 PARTITION OF WEATHER_DATA_PARTITION
                      FOR VALUES FROM ('1985-01-01') TO ('1995-01-01');""",
                        """ CREATE TABLE IF NOT EXISTS measurement_date_1995_to_2005 PARTITION OF WEATHER_DATA_PARTITION
                      FOR VALUES FROM ('1995-01-01') TO ('2005-01-01');""",
                      """ CREATE TABLE IF NOT EXISTS  measurement_date_2005_to_2015 PARTITION OF WEATHER_DATA_PARTITION
                      FOR VALUES FROM ('2005-01-01') TO ('2015-01-01');"""
                        ]

for part_que in range(len(PARTITION_QUERY_LIST)):
    print(f"Executing query..{PARTITION_QUERY_LIST[part_que]}")
    cursor.execute(PARTITION_QUERY_LIST[part_que])
    
print("Table created successfully........")
conn.commit()
#Closing the connection
conn.close()
# Program completed