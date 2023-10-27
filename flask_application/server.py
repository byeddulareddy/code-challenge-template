from flask import Flask ,render_template,request, jsonify
import psycopg2
from datetime import date
from flask_sqlalchemy import SQLAlchemy


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


@app.route('/')
def index():
  conn=db_login()
  cur=conn.cursor()
  cur.execute("select * from weather_data limit 10")
  weather_data=cur.fetchall()
  cur.close()
  conn.close()
  conn=db_login()
  cur=conn.cursor()
  cur.execute("select * from WEATHER_AGGREGATE_DATA limit 10")
  weather_aggregate_data=cur.fetchall()
  cur.close()
  conn.close()
  return render_template('index.html', weather_data=weather_data,weather_aggregate_data=weather_aggregate_data)
 

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

