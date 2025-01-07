from pyspark.sql import SparkSession
from pyspark.sql.functions import *


# Read in dataframe
spark = SparkSession.builder.appName("pyspark tutorial").getOrCreate()
df = spark.read.csv("./data.csv", header='true')
original_df = df

# TODO: look at the data
# df.show()
# df.select("city", "age").show()
# df.select("product").where(col("price") > 10).show()

# Lazily executed

# # TODO: Remove whitespace from strings
# df = df.withColumn("name", trim(df.name))
# df = df.withColumn("city", trim(df.city))
# df = df.withColumn("product", trim(df.product))
# df = df.withColumn("price", trim(df.price))


# # TODO: remove pound symbols and make sure the price column has the right type
# df = df.withColumn("price", regexp_replace('price', '£', ''))
# df = df.withColumn("price", df.price.cast("float"))

df = df.withColumn("price", regexp_replace('price', '£', '').cast("float"))
# print(df.dtypes)

# # TODO: Correct inconsistent capitalizations on products and cities. Use Title Case
# df = df.withColumn('product', initcap(col('product')))
# df = df.withColumn('city', initcap(df['city']))
# df = df.withColumn('name', initcap(df.name))

# # TODO: Handle NaNs / empty cells. Maybe only for certain columns. e.g. don't drop if Nan in city
# original_count = df.count()
# new_count = df.filter(col('name').isNotNull()).count()

# print(f"Original Count: {original_count}")
# print(f"New Count: {new_count}")

# # TODO: remove impossible values (Age should be no higher than 100)
df = df.filter(col('age') <= 100)
df.show()

# # TODO: split out names into first and last names
# def get_first_name(name: str) -> str:
#     nameArray = name.split(' ')
#     if len(nameArray) < 2:
#         return ""
#     return nameArray[0]

# def get_last_name(name: str) -> str:
#     nameArray = name.split(' ')
#     if len(nameArray) < 2:
#         return ""
#     return nameArray[-1]

# df['first_name'] = df['name'].apply(get_first_name)
# df['last_name'] = df['name'].apply(get_last_name)
# df = df[df.first_name != ""]
# df = df.drop(columns=['name'])

# df.to_csv("clean_data.csv", index=False)
# # TODO: duplicate rows (would need to add timestamps)
# # TODO: Title case first and last name