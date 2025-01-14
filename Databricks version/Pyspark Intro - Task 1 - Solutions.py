# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Pyspark Basics - cleaning data
# MAGIC
# MAGIC Create, attach, run cluster
# MAGIC Run the first block of code with the arrow in the top left to read data into a dataframe

# COMMAND ----------

# import additional functions provided by pyspark
from pyspark.sql.functions import *

# COMMAND ----------

# read the table you created as a dataframe
df = spark.read.table('task1_input')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Look at the data

# COMMAND ----------

# Print a whole dataframe with df.display()
df.display()

# COMMAND ----------

# Select and show only 2 columns
df.select('city', 'age').display()

# COMMAND ----------

# filter columns to see only certain rows
# there are many ways to access a column, chose whichever you prefer
df.select("product").where(col("price") > 10).display()
# or 
df.select("product").filter(col("price") > 10).display()
# or
df.select("product").filter(df['price'] > 10).display()
# or 
df.select("product").filter(df.price > 10).display()

# COMMAND ----------

# Add a new column using withColumn
# lit adds the same 'literal' to every row of the column
df.withColumn("new_column", lit("add this string to every cell")).display()

# COMMAND ----------

# Remove a column with drop
# You can chain multiple methods by adding them to the line with dot notation
df.drop('city').drop('age').display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Remove whitespace from strings
# MAGIC Some of our columns have extra whitespace that need removing. For instance "London", "  London", and "London  " are all different
# MAGIC https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.trim.html
# MAGIC

# COMMAND ----------

df = df.withColumn("name", trim(df.name))
df = df.withColumn("city", trim(df.city))
df = df.withColumn("product", trim(df.product))
df = df.withColumn("price", trim(df.price))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### remove pound symbols
# MAGIC - Some of the prices have been supplied as just numbers, some have pound symbols. Let's use only the numbers and remove the pound symbol from any value in the price column
# MAGIC - `£15.6 => 15.6`
# MAGIC - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.regexp_replace.html
# MAGIC
# MAGIC #### make sure the price column has the right type
# MAGIC - Each column has a data type such a string, boolean, float, and double. 
# MAGIC - The price is currently a string but should be a float
# MAGIC - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.cast.html
# MAGIC
# MAGIC #### Challenge: do both of these transformations on the same line
# MAGIC
# MAGIC Hint: you can see the column type of all columns in your dataframe by running `print(df.dtypes)`

# COMMAND ----------

# Seperate version
df = df.withColumn("price", regexp_replace('price', '£', ''))
df = df.withColumn("price", df.price.cast("float"))
print(df.dtypes)

# COMMAND ----------

# Oneline Version - Chaining methods
df = df.withColumn("price", regexp_replace('price', '£', '').cast("float"))
print(df.dtypes)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Correct inconsistent capitalizations
# MAGIC - Some of our data is in all caps, some people have forgotten to capitalize the first letter of their names
# MAGIC - Let's correct this by using initcap on the `product`, `city`, and `name` columns
# MAGIC - Initicap will make the first letter of each word capital and the rest lower case
# MAGIC https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.initcap.html
# MAGIC

# COMMAND ----------

df = df.withColumn('product', initcap(col('product')))
df = df.withColumn('city', initcap(df['city']))
df = df.withColumn('name', initcap(df.name))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### We can only keep records with accurate data. Remove all the rows where no name was provided
# MAGIC https://sparkbyexamples.com/pyspark/pyspark-isnull/
# MAGIC - hint: Check back to earlier steps to see how to filter out rows from a dataframe

# COMMAND ----------

original_count = df.count()
df = df.filter(col('name').isNotNull())
new_count = df.count()

print(f"Original Count: {original_count}")
print(f"New Count: {new_count}")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Handle invalid or impossible values
# MAGIC Some people have negative ages, and one person is over 500 years old! Let's set a minimum age of 0 and a maximum of 100 and remove any rows breaking these rules
# MAGIC
# MAGIC Hint: combine multiple conditions with the & operator or chain 2 filters
# MAGIC

# COMMAND ----------


original_count = df.count()
df = df.filter((col('age') <= 100) & (col('age') >= 0))
# OR
# df = df.filter(col('age') <= 100).filter(col('age') >= 0)
new_count = df.count()

print(f"Original Count: {original_count}")
print(f"New Count: {new_count}")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Challenged: split out names into first and last names using UDFs
# MAGIC https://docs.databricks.com/en/udf/index.html
# MAGIC
# MAGIC hint: You'll need to define 2 functions that can get a first name and a last name, register them as udfs, then use them on the name column

# COMMAND ----------

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

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Write your dataframe to a new table. Use the mode() and saveAsTable() methods
# MAGIC - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.write.html
# MAGIC - https://sparkbyexamples.com/spark/spark-write-options/

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("task1_output")
