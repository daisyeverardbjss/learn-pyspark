# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Pyspark Intro
# MAGIC
# MAGIC - Click Connect in the top right and select your cluster to attach it to this notebook
# MAGIC - You may need to wait a few minutes for it to spin up
# MAGIC
# MAGIC - Click the play arrow in the top left of each cell to run the code in it

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1 - Cleaning Data

# COMMAND ----------

# import additional functions provided by pyspark
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, FloatType
from pyspark.sql import Column

# COMMAND ----------

# read the table you created as a dataframe
# You name need to change the name of the table depending on what yours was called
df = spark.read.table('task1_input')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Examples

# COMMAND ----------

# Print a whole dataframe with df.display()
df.display()

# COMMAND ----------

# Select and show only 2 columns
df.select('city', 'age').display()

# COMMAND ----------

# filter columns to see only certain rows
# there are many ways to access a column, chose whichever you prefer
df.where(F.col("price") > 10).display()
# or 
df.filter(F.col("price") > 10).display()
# or
df.filter(df['price'] > 10).display()
# or 
df.filter(df.price > 10).display()

# COMMAND ----------

# Add a new column using withColumn
# lit adds the same 'literal' to every row of the column
df.withColumn("new_column", F.lit("add this string to every cell")).display()

# COMMAND ----------

# Remove a column with drop
# You can chain multiple methods by adding them to the line with dot notation
df.drop('city').drop('age').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tasks

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Remove whitespace from strings
# MAGIC Some of our columns have extra whitespace that need removing. For instance "London", "  London", and "London  " are all different
# MAGIC Trim the whitespaces from name, city, product, and price
# MAGIC https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.trim.html
# MAGIC

# COMMAND ----------

df_stripped_price = df.TODO

df_stripped_price.display()

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

df_float_price = df_stripped_price.TODO
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

df_capitalised = df_float_price.TODO

df_capitalised.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### We can only keep records with accurate data. Remove all the rows where no name was provided
# MAGIC https://sparkbyexamples.com/pyspark/pyspark-isnull/
# MAGIC - hint: Check back to earlier steps to see how to filter out rows from a dataframe

# COMMAND ----------

original_count = df_capitalised.count()

# CHANGE CODE AFTER THIS LINE
# ------------------------------
df_no_null_names = df_capitalised.TODO
# ------------------------------
# CHANGE CODE BEFORE THIS LINE

new_count = df_no_null_names.count()
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


original_count = df_no_null_names.count()

# CHANGE CODE AFTER THIS LINE
# ------------------------------
df_filtered_ages = df_no_null_names.TODO
# ------------------------------
# CHANGE CODE BEFORE THIS LINE

new_count = df_filtered_ages.count()

print(f"Original Count: {original_count}")
print(f"New Count: {new_count}")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Challenge: split out names into first and last names using UDFs
# MAGIC https://docs.databricks.com/en/udf/index.html
# MAGIC
# MAGIC hint: You'll need to define 2 functions that can get a first name and a last name, register them as udfs, then use them on the name column
# MAGIC - UDFs are generally best avoided where possible. The are a very inefficient method of data processing 

# COMMAND ----------

def get_first_name(name: str) -> str:
    TODO

def get_last_name(name: str) -> str:
    TODO

get_first_name_udf = TODO
get_last_name_udf = TODO

df_split_name = df_filtered_ages.TODO

df_split_name.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Use a column transform functions
# MAGIC The functions are also custom functions applied to a column, but are much more efficient than UDFs
# MAGIC

# COMMAND ----------

def get_first_name(name_col: Column) -> Column:
    TODO

def get_last_name(name_col: Column) -> Column:
    TODO

df_split_name = df_filtered_ages.TODO

df_split_name.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Write your dataframe to a new table. Use the mode() and saveAsTable() methods
# MAGIC - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.write.html
# MAGIC - https://sparkbyexamples.com/spark/spark-write-options/

# COMMAND ----------

# SAVE DATAFRAME AS TABLE HERE

# COMMAND ----------

# MAGIC %md
# MAGIC # Part 2 - Answer the Questions
# MAGIC #### Use pyspark methods on your dataframe to find answers to these analytic questions about the data

# COMMAND ----------

# MAGIC %md
# MAGIC Aggregating data means combining data into groups. There are various types of aggregations including:
# MAGIC - Max
# MAGIC - Min
# MAGIC - Avg
# MAGIC - Sum
# MAGIC
# MAGIC You can use a dictionary with the column to aggregate as the key and a method
# MAGIC - `df.groupBy("department").agg({"salary":"max"})`
# MAGIC
# MAGIC You can do the same thing with an aggregate function on a column
# MAGIC - `df.groupBy("department").agg(max("salary").alias("highest_salary"))`
# MAGIC
# MAGIC Alias renames a column
# MAGIC
# MAGIC https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.agg.html

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Where was the most expensive ice cream bought?

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #### What was the average price of items bought by George Black?
# MAGIC

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #### How many items were bought in Manchester?

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #### Which person spent the least money overall?

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #### How much of the price was VAT?
# MAGIC - Add a new column to show the VAT amount
# MAGIC - Assume that VAT is 20% of the price
# MAGIC - Show the amount to the nearest penny

# COMMAND ----------


