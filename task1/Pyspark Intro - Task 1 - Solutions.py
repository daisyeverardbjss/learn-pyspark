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
# there are many ways to access a column, choose whichever you prefer
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
# MAGIC Some of our columns have extra whitespace that need removing. For instance "London", "  London", and "London  " are all different<br>
# MAGIC Trim the whitespaces from name, city, product, and price<br>
# MAGIC <br>
# MAGIC Trim is part of the functions package which we have imported as F. This means you have to use it as  `F.trim(<column to trim>)`<br>
# MAGIC The trim function takes an argument of type column, and also returns a column.<br>
# MAGIC Once you've got the new column content, you'll need to overwrite the old column using `withColumn()`<br>
# MAGIC <br>
# MAGIC Official documentation to relevant methods is linked, but you may find it easier to find clear examples elsewhere<br>
# MAGIC https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.trim.html<br>
# MAGIC

# COMMAND ----------

df_stripped_name = df.withColumn("name", F.trim(df.name))
df_stripped_city = df.withColumn("city", F.trim(df.city))
df_stripped_product = df.withColumn("product", F.trim(df.product))
df_stripped_price = df.withColumn("price", F.trim(df.price))
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

# Seperate version
df_removed_pound = df_stripped_price.withColumn("price", F.regexp_replace('price', '£', ''))
print(df_removed_pound.dtypes)
df_float_price = df.withColumn("price", df.price.cast("float"))
print(df_float_price.dtypes)

# COMMAND ----------

# Oneline Version - Chaining methods
print(df_stripped_price.dtypes)
df_float_price = df_stripped_price.withColumn("price", F.regexp_replace('price', '£', '').cast("float"))
print(df_float_price.dtypes)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Correct inconsistent capitalizations
# MAGIC - Some of our data is in all caps, some people have forgotten to capitalize the first letter of their names
# MAGIC - Let's correct this by using initcap on the `product`, `city`, and `name` columns
# MAGIC - Initicap will make the first letter of each word capital and the rest lower case
# MAGIC https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.initcap.html
# MAGIC

# COMMAND ----------

df_capitalised = (df_float_price
                  .withColumn('product', F.initcap(F.col('product')))
                  .withColumn('city', F.initcap(df['city']))
                  .withColumn('name', F.initcap(df.name)))
df_capitalised.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### We can only keep records with accurate data. Remove all the rows where no name was provided
# MAGIC https://sparkbyexamples.com/pyspark/pyspark-isnull/
# MAGIC - hint: Check back to earlier steps to see how to filter out rows from a dataframe

# COMMAND ----------

original_count = df_capitalised.count()
df_no_null_names = df_capitalised.filter(F.col('name').isNotNull())
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
df_filtered_ages = df_no_null_names.filter((F.col('age') <= 100) & (F.col('age') >= 0))
# OR
# df = df.filter(col('age') <= 100).filter(col('age') >= 0)
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

df_with_first_name = df_filtered_ages.withColumn("first_name", get_first_name_udf(F.col("name")))
df_with_last_name = df_with_first_name.withColumn("last_name", get_last_name_udf(F.col("name")))
df_split_name = df_with_last_name.filter(F.col('first_name') != "").drop("name")

df_split_name.display()

# COMMAND ----------

def get_first_name(name_col: Column) -> Column:
    return F.when(name_col.isNotNull() & (F.size(F.split(name_col, ' ')) >= 2), F.split(name_col, ' ')[0]).otherwise("")

def get_last_name(name_col: Column) -> Column:
    return F.when(name_col.isNotNull() & (F.size(F.split(name_col, ' ')) >= 2), F.split(name_col, ' ')[-1]).otherwise("")

df_with_first_name = df_filtered_ages.withColumn("first_name", get_first_name(F.col("name")))
df_with_last_name = df_with_first_name.withColumn("last_name", get_last_name(F.col("name")))
df_split_name = df_with_last_name.filter(F.col('first_name') != "").drop("name")

df_split_name.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Write your dataframe to a new table. Use the mode() and saveAsTable() methods
# MAGIC - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.write.html
# MAGIC - https://sparkbyexamples.com/spark/spark-write-options/

# COMMAND ----------

df_split_name.write.mode("overwrite").saveAsTable("task1_output")

# COMMAND ----------

# MAGIC %md
# MAGIC # Part 2 - Anlysis
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

#  1 - filter then find max
ice_creams_df = df_split_name.where(F.col('product').like("%Ice Cream%"))
ice_creams_df.select(F.max(ice_creams_df.price).alias("most_expensive"), 
          F.min(ice_creams_df.price).alias("least_expensive")
    ).display()

# COMMAND ----------


# 2 - see all max prices
df_split_name.groupBy("product").agg({"price":"max"}).withColumnRenamed("max(price)", "max_price").orderBy("max_price", ascending=False).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### What was the average price of items bought by George Black?
# MAGIC

# COMMAND ----------

# 1 - filter for george black first then find only their average
george_df = df_split_name.where(F.col('first_name') == "George").where(F.col('last_name') == "Black")
george_df.select(F.avg(F.col('price'))).withColumn("average_price", F.round(F.col("avg(price)"), 2)).display()


# COMMAND ----------

# 2 - see all averages
df_split_name.groupBy("first_name", "last_name").agg({"price":"avg"}).withColumn("average_price", F.round(F.col("avg(price)"), 2)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### How many items were bought in Manchester?

# COMMAND ----------

total = df_split_name.where(F.col('city') == "Manchester").count()
print(f"total: {total}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Which person spent the least money overall?

# COMMAND ----------

(df_split_name.groupBy("first_name", "last_name")
 .agg({"price":"sum"})
 .withColumn("total_price", F.round(F.col("sum(price)"), 2))
 .orderBy("total_price")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### How much of the price was VAT?
# MAGIC - Add a new column to show the VAT amount
# MAGIC - Assume that VAT is 20% of the price
# MAGIC - Show the amount to the nearest penny

# COMMAND ----------

# Column transform version: This way is better
def get_vat_column(price_column: Column) -> Column:
    VAT_RATE = 0.2
    return F.round(price_column * 0.2, 2)

df_split_name.withColumn("VAT", get_vat_column(F.col('price'))).display()

# COMMAND ----------

# UDF version
def get_vat_amount(total_price: float) -> float:
    VAT_RATE = 0.2
    return round(total_price * VAT_RATE, 2)

get_vat_amount_udf = udf(get_vat_amount, FloatType())

df_split_name.withColumn("VAT", get_vat_amount_udf(F.col('price'))).display()
