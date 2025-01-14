from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType


# Read in dataframe
spark = SparkSession.builder.appName("pyspark tutorial").getOrCreate()
df = spark.read.csv("./data.csv", header='true')

# EXAMPLES

# Print a whole dataframe with df.display()
df.display()

# Select and show only 2 columns
df.select('city', 'age').display()

# filter columns to see only certain rows
# there are many ways to access a column, chose whichever you prefer
df.select("product").where(col("price") > 10).display()
# or 
df.select("product").filter(col("price") > 10).display()
# or
df.select("product").filter(df['price'] > 10).display()
# or 
df.select("product").filter(df.price > 10).display()

# Add a new column using withColumn
# lit adds the same 'literal' to every row of the column
df.withColumn("new_column", lit("add this string to every cell")).display()

# Remove a column with drop
# You can chain multiple methods by adding them to the line with dot notation
df.drop('city').drop('age').display()

# TASKS START HERE

# # TODO: Remove whitespace from strings
df = df.withColumn("name", trim(df.name))
df = df.withColumn("city", trim(df.city))
df = df.withColumn("product", trim(df.product))
df = df.withColumn("price", trim(df.price))

# # TODO: remove pound symbols and make sure the price column has the right type
# seperate lines version
df = df.withColumn("price", regexp_replace('price', '£', ''))
df = df.withColumn("price", df.price.cast("float"))
print(df.dtypes)

# oneline version
df = df.withColumn("price", regexp_replace('price', '£', '').cast("float"))
print(df.dtypes)

# # TODO: Correct inconsistent capitalizations on products and cities. Use Title Case
df = df.withColumn('product', initcap(col('product')))
df = df.withColumn('city', initcap(df['city']))
df = df.withColumn('name', initcap(df.name))

# # TODO: Handle NaNs / empty cells. Maybe only for certain columns. e.g. don't drop if Nan in city
original_count = df.count()
df = df.filter(col('name').isNotNull())
new_count = df.count()

print(f"Original Count: {original_count}")
print(f"New Count: {new_count}")

# # TODO: remove impossible values (Age should be no higher than 100)
df = df.filter(col('age') <= 100)

# # TODO: split out names into first and last names
def get_first_name(name: str) -> str:
    if name is None:
        return ""
    nameArray = name.split(' ')
    if len(nameArray) < 2:
        return ""
    return nameArray[0]

get_first_name_udf = udf(lambda x:get_first_name(x),StringType())

def get_last_name(name: str) -> str:
    if name is None:
        return ""
    nameArray = name.split(' ')
    if len(nameArray) < 2:
        return ""
    return nameArray[-1]

get_last_name_udf = udf(lambda x:get_last_name(x),StringType())

df = df.withColumn("first_name", get_first_name_udf(col("name")))
df = df.withColumn("last_name", get_last_name_udf(col("name")))
df = df.filter(col('first_name') != "").drop("name")

df.write.format("csv").mode("overwrite").save("clean_data")