# pylint: disable=bad-indentation
from flask import Flask ,render_template,request, jsonify
import psycopg2
from datetime import date
from flask_sqlalchemy import SQLAlchemy
from flask_paginate import Pagination

app = Flask(__name__, template_folder="template")

user='postgres'
password='password'
host='localhost'
connection_string='postgresql://{}:{}@{}:5432/corteva'.format(user,password,host)
app.config['SQLALCHEMY_DATABASE_URI']=connection_string

db=SQLAlchemy(app)

def db_login():
    db_credentials = {
    "user": "postgres",
    "password": "password",
    "host": "localhost",
    "port": "5432",
    "database": "corteva"}
    # Connect to the PostgreSQL database
    connection = psycopg2.connect(**db_credentials)
    return connection

def execute_queries(query):
    try:
        conn=db_login()
        cur=conn.cursor()
        cur.execute(query)
        data=cur.fetchall()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    finally:
        # Close the cursor and connection
        if cur:
            cur.close()
        if conn:
            conn.close() 
    return data

@app.route('/')
def index():
    page = request.args.get('page', 1, type=int)
    per_page = 10  # Number of items per page
    weather_query="""select weather_station_id,measurement_date,max_temperature,min_temperature,precipitation 
                     from WEATHER_DATA """
    weather_data=execute_queries(weather_query)
    # weather_count=len(weather_data)
    weather_start_page = (page - 1) * per_page
    weather_end_page = weather_start_page + per_page
    weather_paginated_data = weather_data[weather_start_page:weather_end_page]
    # pagination = Pagination(page=page, total=weather_count, record_name='items', per_page=per_page)
    weather_total_pages = len(weather_data) // per_page + (1 if len(weather_data) % per_page > 0 else 0)
    # print(f"pagination..{pagination.pages}")
    weather_aggregate_query="""select weather_station_id,measurement_year,avg_max_temperature,avg_min_temperature,total_precipitation
                     from WEATHER_AGGREGATE_DATA """
    weather_aggregate_data=execute_queries(weather_aggregate_query)
    # weather_agg_count=len(weather_data)
    weather_agg_start_page = (page - 1) * per_page
    weather_agg_end_page = weather_agg_start_page + per_page
    weather_agg_paginated_data = weather_aggregate_data[weather_agg_start_page:weather_agg_end_page]
    weather_agg_total_pages = len(weather_aggregate_data) // per_page + (1 if len(weather_aggregate_data) % per_page > 0 else 0)
    return render_template('index.html', weather_paginated_data=weather_paginated_data , weather_total_pages=weather_total_pages, weather_data_current_page=page,
                                         weather_agg_paginated_data=weather_agg_paginated_data , weather_agg_total_pages=weather_agg_total_pages, weather_agg_current_page=page)
 

# add a new sock to the database
@app.route('/api/weather', methods=['GET', 'POST'])
def insert():
    conn=db_login()
    cur=conn.cursor()
    where_condition = ''

    if request.values.get('weather_station_id'):
        weather_station_id = request.values.get('weather_station_id')
        where_condition = where_condition + f" AND weather_station_id='{weather_station_id}'"
    
    if request.values.get('measurement_date'):
        measurement_date = request.values.get('measurement_date')
        where_condition = where_condition + f" AND measurement_date='{measurement_date}'"
    
    cur.execute(f"select * from weather_data where 1=1 {where_condition}")
    data=cur.fetchall()

    cur.close()
    conn.close()
    # render_template('index.html', data=data)
    return  jsonify(
                      weather_data=data
                  )

# add a new sock to the database
@app.route('/api/weather/stats', methods=['GET', 'POST'])
def weather_stats():
    conn=db_login()
    cur=conn.cursor()
    where_condition = ''

    if request.values.get('weather_station_id'):
        weather_station_id = request.values.get('weather_station_id')
        where_condition = where_condition + f" AND weather_station_id='{weather_station_id}'"
    
    if request.values.get('measurement_year'):
        measurement_year = request.values.get('measurement_year')
        where_condition = where_condition + f" AND measurement_year='{measurement_year}'"
    
    cur.execute(f"select * from WEATHER_AGGREGATE_DATA where 1=1 {where_condition}")
    WEATHER_AGGREGATE_DATA=cur.fetchall()
    cur.close()
    conn.close()
    # render_template('index.html', data=WEATHER_AGGREGATE_DATA)
    return jsonify(
                      WEATHER_AGGREGATE_DATA=WEATHER_AGGREGATE_DATA
                  )
if __name__ == "__main__":
    app.run(debug=True)

