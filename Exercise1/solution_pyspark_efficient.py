from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType


# Read in dataframe
spark = SparkSession.builder.appName("pyspark tutorial").getOrCreate()
df = spark.read.csv("./data.csv", header='true')
original_df = df

df = df.withColumn("name", trim(df.name))
df = df.withColumn("city", trim(df.city))
df = df.withColumn("product", trim(df.product))
df = df.withColumn("price", trim(df.price).regexp_replace('price', '£', '').cast("float"))

display(df)
