import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType
from pyspark.sql.functions import regexp_replace,col
import sys
import configparser

file_path=sys.argv[1]
config_path=sys.argv[2]
table_name=sys.argv[3]
# Create SparkSession
spark = SparkSession.builder.getOrCreate()

# Load data from CSV file to data frame

df = spark.read.csv(file_path, header=True, inferSchema=True)

# Count rows to check the data
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

#connection to Mysql
config = configparser.ConfigParser()
config.read(config_path)

mysql_url = config['mysql']['url']
mysql_driver = config['mysql']['driver']
mysql_user = config['mysql']['user']
mysql_password = config['mysql']['password']



mysql_properties = {
        "driver": mysql_driver,
        "user": mysql_user,
        "password": mysql_password
}
#writing data into database

df.write.jdbc(url=mysql_url, table=table_name, mode="overwrite", properties=mysql_properties)


# Stop SparkSession
spark.stop()