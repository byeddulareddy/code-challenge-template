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


@app.route('/api/weather')
def index():
  conn=db_login()
  cur=conn.cursor()
  cur.execute("select * from weather_data limit 100")
  weather_data=cur.fetchall()
  cur.close()
  conn.close()
  return  render_template('weather_index.html', weather_data=weather_data)
 
