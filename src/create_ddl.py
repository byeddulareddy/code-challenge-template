import psycopg2

localhost='localhost'

#Establishing the connection
conn = psycopg2.connect(
   database="corteva", user='postgres', password='password', host=localhost, port= '5432'
)
#Creating a cursor object using the cursor() method
cursor = conn.cursor()

#Doping WEATHER_DATA table if already exists.
cursor.execute("DROP TABLE IF EXISTS WEATHER_DATA")

#Creating table as per requirement with no partitions
create_weather_data_table ='''CREATE TABLE WEATHER_DATA(
                                                   weather_station_id VARCHAR(100),
                                                   measurement_date DATE NOT NULL,
                                                   max_temperature INTEGER,
                                                   min_temperature INTEGER,
                                                   precipitation INTEGER,
                                                  primary key (weather_station_id, measurement_date) 
                                                );
                           '''
cursor.execute(create_weather_data_table)

#Doping WEATHER_DATA_PARTITION table if already exists.
cursor.execute("DROP TABLE IF EXISTS WEATHER_DATA_PARTITION")
create_weather_data_partition_table ='''CREATE TABLE WEATHER_DATA_PARTITION(
                                                   weather_station_id VARCHAR(100),
                                                   measurement_date DATE NOT NULL,
                                                   max_temperature INTEGER ,
                                                   min_temperature INTEGER,
                                                   precipitation INTEGER,
                                                   primary key (weather_station_id, measurement_date) 
                                                )
                              PARTITION BY RANGE (measurement_date);
                           '''
cursor.execute(create_weather_data_partition_table)

partition_query_list=[""" CREATE TABLE measurement_date_1985_to_1995 PARTITION OF WEATHER_DATA_PARTITION
                      FOR VALUES FROM ('1985-01-01') TO ('1995-01-01');""",
                        """ CREATE TABLE measurement_date_1995_to_2005 PARTITION OF WEATHER_DATA_PARTITION
                      FOR VALUES FROM ('1995-01-01') TO ('2005-01-01');""",
                      """ CREATE TABLE measurement_date_2005_to_2015 PARTITION OF WEATHER_DATA_PARTITION
                      FOR VALUES FROM ('2005-01-01') TO ('2015-01-01');"""
                        ]

for part_que in range(len(partition_query_list)):
    print(f"Executing query..{partition_query_list[part_que]}")
    cursor.execute(partition_query_list[part_que])
    
print("Table created successfully........")
conn.commit()
#Closing the connection
conn.close()