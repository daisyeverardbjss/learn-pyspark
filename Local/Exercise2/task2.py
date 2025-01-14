from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType


spark = SparkSession.builder.appName("pyspark tutorial").getOrCreate()
df = spark.read.csv("./data.csv", header='true')
original_df = df

# Where was the most expensive ice cream bought?
# #  1 - filter then find max
# ice_creams_df = df.where(col('product').like("%Ice Cream%"))
# df.select(max(df.price).alias("most_expensive"), 
#           min(df.price).alias("least_expensive")
#     ).show()

# # 2 - see all max prices
# df.groupBy("product").agg({"price":"max"}).show()

# What was the average price of item bought by George Black?
# # 1 - filter for george black first then find only their average
# george_df = df.where(col('first_name') == "George").where(col('last_name') == "Black")
# george_df.select(avg(col('price'))).show()

# # 2 - see all averages
# df.groupBy("first_name", "last_name").agg({"price":"avg"}).show()

# How many items were bought in Manchester?
total = df.where(col('city') == "Manchester").count()
print(f"total: {total}")

# Which person spent the most money overall?
df.groupBy().agg({"price":"sum"}).show()
# Which person spent the least money overall?
#   group, count

# Add a new column to display how much of the price was VAT. Assume VAT is 20% and show the price to the nearest penny
#   new column, lambda? 