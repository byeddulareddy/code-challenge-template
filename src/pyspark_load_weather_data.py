from pyspark.sql import SparkSession
from pyspark import SparkContext
import os
from pyspark.sql.functions import input_file_name

spark = SparkSession.builder \
      .appName("SparkByExample") \
      .getOrCreate();
sc=spark.sparkContext

jdbc_url = "jdbc:postgresql://localhost:5432/corteva"
connection_properties = {
    "user": "postgres",
    "password": "password",
    "driver": "org.postgresql.Driver"
}


path="C:\\Users\Bhargava Reddy Yeddu\Downloads\corteva\code-challenge-template\wx_data\*"
# Create a new column 'filename' with the name of the input file
# Convert the RDD to a DataFrame
rdd = sc.textFile("C://Users/Bhargava Reddy Yeddu/Downloads/corteva/code-challenge-template/wx_data/")
print(rdd)
df = rdd.toDF(["content"])

