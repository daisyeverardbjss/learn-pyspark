from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType


spark = SparkSession.builder.appName("pyspark tutorial").getOrCreate()
df = spark.read.csv("./data.csv", header='true')
original_df = df

# Where was the most expensive ice cream bought?
#  filtering, max  
ice_creams_df = df.where(col('product').like("%Ice Cream%"))
df.select(max(df.price).alias("most_expensive"), 
          min(df.price).alias("least_expensive")
    ).show()
# What was the average price of item bought by George Black?
#   averageing, filtering
# How many items were bought in Manchester?
#   filter, count
# Which person spent the most money overall?
# Which person spent the least money overall?
#   group, count

# Add a new column to display how much of the price was VAT. Assume VAT is 20% and show the price to the nearest penny
#   new column, lambda? 