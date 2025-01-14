from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType


# Read in dataframe
spark = SparkSession.builder.appName("pyspark tutorial").getOrCreate()
df = spark.read.csv("./data.csv", header='true')
original_df = df

# Define UDFs
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

# In spark you can chain all of your methods together on one line
# Just because you could doesn't mean you should
df = (df.withColumn("name", initcap(trim(df.name)))
      .withColumn("city", initcap(trim(df.city)))
      .withColumn("product", initcap(trim(df.product)))
      .withColumn("price", regexp_replace(trim(df.price), '£', '').cast("float"))
      .filter(col('name').isNotNull()).filter(col('age') <= 100)
      .withColumn("first_name", get_first_name_udf(col("name")))
      .withColumn("last_name", get_last_name_udf(col("name")))
      .filter(col('first_name') != "").drop("name"))

df.show()
