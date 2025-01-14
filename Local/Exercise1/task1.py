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
# TODO: Remove whitespace from strings
# Some of our columns have extra whitespace that need removing. For instance "London", "  London", and "London  " are all different
# https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.trim.html

# TODO: Some of the prices have been supplied as just numbers, some have pound symbols. Let's use only the numbers and remove the pound symbol from any value in the price column
# e.g. `£15.6 => 15.6`
# https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.regexp_replace.html

# TODO:  make sure the price column has the right type
#  Each column has a data type such a string, boolean, float, and double. 
#  The price is currently a string but should be a float
# https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.cast.html
# Challenge: do both of these transformations on the same line
# Hint: you can see the column type of all columns in your dataframe by running `print(df.dtypes)`

# TODO: Correct inconsistent capitalizations
# Some of our data is in all caps, some people have forgotten to capitalize the first letter of their names
# Let's correct this by using initcap on the `product`, `city`, and `name` columns
# Initicap will make the first letter of each word capital and the rest lower case
# https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.initcap.html

# TODO: We can only keep records with accurate data. Remove all the rows where no name was provided
# https://sparkbyexamples.com/pyspark/pyspark-isnull/
# MAGIC - hint: Check back to earlier steps to see how to filter out rows from a dataframe

original_count = df.count()

# CHANGE CODE AFTER THIS LINE
# ------------------------------
df = df.TODO
# ------------------------------
# CHANGE CODE BEFORE THIS LINE

new_count = df.count()
print(f"Original Count: {original_count}")
print(f"New Count: {new_count}")



# TODO: Handle invalid or impossible values
# Some people have negative ages, and one person is over 500 years old! Let's set a minimum age of 0 and a maximum of 100 and remove any rows breaking these rules
# Hint: combine multiple conditions with the & operator or chain 2 filters

original_count = df.count()

# CHANGE CODE AFTER THIS LINE
# ------------------------------
df = df.TODO
# ------------------------------
# CHANGE CODE BEFORE THIS LINE

new_count = df.count()

print(f"Original Count: {original_count}")
print(f"New Count: {new_count}")

# TODO: Challenge: split out names into first and last names using UDFs
# MAGIC https://docs.databricks.com/en/udf/index.html
# MAGIC hint: You'll need to define 2 functions that can get a first name and a last name, register them as udfs, then use them on the name column

def get_first_name(name: str) -> str:
    #TODO

def get_last_name(name: str) -> str:
    #TODO

get_first_name_udf = TODO
get_last_name_udf = TODO

df = df.TODO

df.display()

# TODO: Write your dataframe to a new CSV file. Use the mode() and save() methods
df.write.format("csv").mode("overwrite").save("clean_data")