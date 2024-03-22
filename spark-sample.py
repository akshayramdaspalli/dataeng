import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType
from pyspark.sql.functions import regexp_replace,col
import sys

# Create SparkSession
spark = SparkSession.builder.getOrCreate()

# Load data from CSV
file_path=r"E:\akki\archive\googleplaystore.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)

# Count rows
row_count = df.count()
print("Number of rows in DataFrame:", row_count)


#trimming the data

df=df.drop("size","Content Rating","Last Updated","Android Ver","Current Ver")
df.show(1)

#Check Schema
df.printSchema()


#Cleansing the data

df=df.withColumn("Installs",regexp_replace(df["Installs"],"[^0-9]",""))\
    .withColumn("Price",regexp_replace(df["Price"],"[$]",""))

df= df.withColumn("Rating", df["Rating"].cast("int"))\
    .withColumn("Reviews",df["Reviews"].cast("int"))\
    .withColumn("Installs",df["Installs"].cast("int"))\
    .withColumn("Price",df["Price"].cast("int"))

#check the updated DataType
df.printSchema()

df.show(1)

#connecting to Mysql

mysql_url = "jdbc:mysql://txpro1.fcomet.com:3306/hanoomac_dataeng"
mysql_properties = {
    "driver": "com.mysql.cj.jdbc.Driver",
    "user": "hanoomac_dataeng",
    "password": "LetsD0C0nnect"
}
#writing data into database

df.write.jdbc(url=mysql_url, table="Google_data", mode="overwrite", properties=mysql_properties)


# Stop SparkSession
spark.stop()