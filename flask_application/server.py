# pylint: disable=bad-indentation
from flask import Flask ,render_template,request, jsonify, abort
from flask import Flask, jsonify, make_response
from flask_cors import CORS
from flask_marshmallow import Marshmallow
import psycopg2
from datetime import date
from flask_sqlalchemy import SQLAlchemy
from flask_paginate import Pagination
from flask_swagger_ui import get_swaggerui_blueprint
from flask_cors import CORS
from flasgger import Swagger

APP = Flask(__name__, template_folder="template")

user='postgres'
password='password'
host='localhost'
connection_string='postgresql://{}:{}@{}:5432/corteva'.format(user,password,host)
APP.config['SQLALCHEMY_DATABASE_URI']=connection_string
CORS(APP)  # Enable CORS for all routes

db=SQLAlchemy(APP)
ma = Marshmallow(APP)


SWAGGER_URL = '/swagger'  # URL for exposing Swagger UI (without trailing '/')
API_URL = '/static/swagger.json'  # Our API url (can of course be a local resource)


swaggerui_blueprint = get_swaggerui_blueprint(
    SWAGGER_URL,  # Swagger UI static files will be mapped to '{SWAGGER_URL}/dist/'
    API_URL,
    config={
        'syntaxHighlight': False,  # Disable syntax highlighting
    }
)

APP.register_blueprint(swaggerui_blueprint,url_prefix=SWAGGER_URL)
### end swagger specific ###
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

@APP.route('/')
def weather_data():
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
    
    return render_template('index.html', weather_paginated_data=weather_paginated_data , weather_total_pages=weather_total_pages, weather_data_current_page=page)
@APP.route('/weatherStats')
def weather_ui_stats():
    page = request.args.get('page', 1, type=int)
    per_page = 10  # Number of items per page
    weather_stats_query="""select weather_station_id,measurement_year,avg_max_temperature,avg_min_temperature,total_precipitation_cm
                     from WEATHER_AGGREGATE_DATA """
    weather_agg_stats_data=execute_queries(weather_stats_query)
    # weather_count=len(weather_stats_query)
    weather_agg_start_page = (page - 1) * per_page
    weather_agg_end_page = weather_agg_start_page + per_page
    weather_agg_paginated_data = weather_agg_stats_data[weather_agg_start_page:weather_agg_end_page]
    # pagination = Pagination(page=page, total=weather_count, record_name='items', per_page=per_page)
    weather_agg_total_pages = len(weather_agg_stats_data) // per_page + (1 if len(weather_agg_stats_data) % per_page > 0 else 0)
    # print(f"pagination..{pagination.pages}")
    
    return render_template('weather_stats.html', weather_agg_paginated_data=weather_agg_paginated_data , weather_agg_total_pages=weather_agg_total_pages, weather_agg_current_page=page)
# add a new sock to the database
@APP.route('/api/weather', methods=['GET','POST'])
def get_weather_data():
    # return {"weather_station_id":"John", "measurement_date":"30", "max_temperature":"null", "min_temperature":"30", "precipitation":"null"}
    try:
        conn=db_login()
        cur=conn.cursor()
        where_condition = ''

        if request.values.get('weather_station_id'):
            weather_station_id = request.values.get('weather_station_id')
            where_condition = where_condition + f" AND weather_station_id='{weather_station_id}'"
        
        if request.values.get('measurement_date'):
            measurement_date = request.values.get('measurement_date')
            where_condition = where_condition + f" AND measurement_date='{measurement_date}'"
        cur.execute(f"select weather_station_id,measurement_date,max_temperature,min_temperature,precipitation  from weather_data where 1=1 {where_condition} ")
        weather_data=cur.fetchall()
        
        # list_of_lists=json.dumps(weather_data, cls=CustomJSONEncoder)
        serialized_data = [{'weather_station_id': item[0], 'measurement_date': item[1],
                            'max_temperature': item[2], 'min_temperature': item[3],
                             'precipitation': item[4]} for item in weather_data]
       
        cur.close()
        conn.close()
        # render_template('index.html', data=data)
        return  get_paginated_list(serialized_data,'/api/weather',start=request.args.get('start', 1), 
                    limit=request.args.get('limit', 2000))        
    

    
    except Exception as e:
        return jsonify({'error': str(e)})
# add a new sock to the database
@APP.route('/api/weather/stats', methods=['GET','POST'])
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
    
    cur.execute(f"select weather_station_id,measurement_year,avg_max_temperature,avg_min_temperature,total_precipitation_cm from WEATHER_AGGREGATE_DATA where 1=1 {where_condition}")
    WEATHER_AGGREGATE_DATA=cur.fetchall()
    serialized_data = [{'weather_station_id': item[0], 'measurement_year': item[1],
                            'avg_max_temperature': item[2], 'avg_min_temperature': item[3],
                             'total_precipitation': item[4]} for item in WEATHER_AGGREGATE_DATA]
    cur.close()
    conn.close()
    return get_paginated_list(serialized_data,'/api/weather/stats',start=request.args.get('start', 1), 
                    limit=request.args.get('limit', 2000)) 
def get_paginated_list(results, url, start, limit):
    start = int(start)
    limit = int(limit)
    count = len(results)
    if count < start or limit < 0:
        abort(404)
    # make response
    obj = {}
    obj['start'] = start
    obj['limit'] = limit
    obj['count'] = count
    # make URLs
    # make previous url
    if start == 1:
        obj['previous'] = ''
    else:
        start_copy = max(1, start - limit)
        limit_copy = start - 1
        obj['previous'] = url + '?start=%d&limit=%d' % (start_copy, limit_copy)
    # make next url
    if start + limit > count:
        obj['next'] = ''
    else:
        start_copy = start + limit
        obj['next'] = url + '?start=%d&limit=%d' % (start_copy, limit)
    # finally extract result according to bounds
    obj['results'] = results[(start - 1):(start - 1 + limit)]
    return obj

if __name__ == "__main__":
    APP.run(debug=True)

