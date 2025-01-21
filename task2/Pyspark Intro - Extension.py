# Databricks notebook source
# MAGIC %md
# MAGIC ## Pyspark Intro - Extension
# MAGIC ## Star Schemas

# COMMAND ----------

# MAGIC %md
# MAGIC #### Useful Links
# MAGIC [Medium Article - SQL joins](https://medium.com/@authfy/seven-join-techniques-in-sql-a65786a40ed3)

# COMMAND ----------

# MAGIC %md
# MAGIC ## What is a Star Schema?
# MAGIC In the next example we'll create a star schema for a dataset and explain why star schemas are used

# COMMAND ----------

# MAGIC %md
# MAGIC Run the next code block to see a small dataset about books before conforming it to a star schema. It contains information about the book, the author, and the publisher.

# COMMAND ----------


data = [
    ["To Kill a Mockingbird", "J.B. Lippincott & Co.", 1960, "Harper Lee", "United States", "1926-04-28"],
    ["1984", "Secker & Warburg", 1949, "George Orwell", "United Kingdom", "1903-06-25"],
    ["The Great Gatsby", "Charles Scribner's Sons", 1925, "F. Scott Fitzgerald", "United States", "1896-09-24"]
]
columns = ["Book Name", "Publisher", "Publication Year", "Author", "Author's Country", "Author Date of Birth"]

df1 = spark.createDataFrame(data, schema="`Book Name` STRING, `Publisher` STRING, `Publication Year` INT, `Author` STRING, `Author's Country` STRING, `Author Date of Birth` STRING")
display(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC A star schema is a way or organising a database with `fact tables` and `dimension_tables`.
# MAGIC <br><br>
# MAGIC A `dimension table` contains descriptive information about an entities. Each row will have a unique `primary key` to identify it. In our example we will use the numbers 1,2, and 3 as our primary keys but they can also be a combination of multiple columns called `composite keys`. For instance dim_employee, a dimension table containing information about employees, would have a row for each employee and column for each attribute such as age, salary, job title
# MAGIC <br><br>
# MAGIC A `fact_table` combines attributes from dimension tables. It will contain `foreign keys` which match up with the `primary keys` from the dimension tables. fact_sales might be a table to track sales in a shop where each row is a single sale. The columns might be item_id, customer_id, employee_id
# MAGIC <br><br>
# MAGIC  For our book database, if we had a larger dataset we would find the country, years, author, and publisher may all be deuplicated many times.
# MAGIC  The natural entities in this dataset are the publisher, author, and book.
# MAGIC  We'll split the data into one `fact_table` with `foreign keys` linking to our 3 `dimension tables` for each entity.
# MAGIC  The book, publisher, and author `dimension tables` with have a new `primary key` which is used to link to the foreign keys in the fact table

# COMMAND ----------

# DATA
book_data = [
    [1, "To Kill a Mockingbird", 1960],
    [2, "1984", 1949],
    [3, "The Great Gatsby", 1925]
]
fact_data = [
    [1, 1, 1],  # Book ID, Author ID, Publisher ID
    [2, 2, 2],
    [3, 3, 1]
]
authors_data = [
    [1, "Harper Lee", "United States", "1926-04-28"],
    [2, "George Orwell", "United Kingdom", "1903-06-25"],
    [3, "F. Scott Fitzgerald", "United States", "1896-09-24"]
]
publishers_data = [
    [1, "J.B. Lippincott & Co."],
    [2, "Secker & Warburg"],
    [3, "Charles Scribner's Sons"]
]

# SCHEMAS
fact_books = spark.createDataFrame(fact_data, schema="`Book ID` INT, `Author ID` INT, `Publisher ID` INT")
dim_books = spark.createDataFrame(book_data, schema="`Book ID` INT, `Book Name` STRING, `Publication Year` INT")
dim_author = spark.createDataFrame(authors_data, schema="`Author ID` INT, `Author Name` STRING, `Author's Country` STRING, `Author Date of Birth` STRING")
dim_publisher = spark.createDataFrame(publishers_data, schema="`Publisher ID` INT, `Publisher Name` STRING")

# DISPLAY
display(fact_books)
display(dim_books)
display(dim_author)
display(dim_publisher)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Why?
# MAGIC ###### Single source of truth:
# MAGIC  If you need to change one of the details, e.g. there is a typo in the publisher name, it only needs to be changed once instead of on every row
# MAGIC
# MAGIC ###### Less redundancy:
# MAGIC  There is less duplication of the data which can reduce storage space required
# MAGIC
# MAGIC ###### Querying:
# MAGIC  Fewer joins are required to query data, meaning easier and faster reads
# MAGIC
# MAGIC ###### Scalability:
# MAGIC It's easy to add rows to a dim or fact table to represent a new entity or a new event involving existing entities

# COMMAND ----------

# MAGIC %md
# MAGIC #### Considerations
# MAGIC - Schema design can be complex and there are not always obvious entities to split out into dimension tables
# MAGIC - Understanding how to query data using joins can be confusing for new users, especially when the columns included in each table are not obvious
# MAGIC - A star schema is generally better suited to large datasets
# MAGIC - It can be hard to track changing dimensions in a star schema due to the 'one source of truth' model. 
# MAGIC Imagine a shop which uses a dimension table to track to cost of each item they buy from a supplier. If the supplier changes the cost of an item, they will update the dimension table with the new cost. They will not then be able to find out how much the same order would have cost 1 year ago as they have overwritten the price data with the newer price.
# MAGIC One way to solve this is with multiple rows in the dimension table for each item and a composite primary key which inculdes the item ID, start date of that row's price, and end date of that row's price

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task - Planet Data
# MAGIC If you haven't already, upload the CSV's provided in the repo for this project to databricks and read them in
# MAGIC
# MAGIC A `union` is adding rows from two different tables together to make one larger table with the same schema. 
# MAGIC In the datasets provided, both tables have the same columns in the same order, so they can be unioned successfully.
# MAGIC https://sparkbyexamples.com/pyspark/pyspark-union-and-unionall/

# COMMAND ----------

# Read in and combine the two data sources

# COMMAND ----------

# MAGIC %md
# MAGIC Now we need to create the star schema.
# MAGIC Identify which entities you want to turn into dimension tables, and which columns should go into each table
# MAGIC When you split the data into 3 dataframe, you will also need to create keys to link the dim and fact tables

# COMMAND ----------

# Create fact and dimension tables from the data
# Good Luck

# COMMAND ----------

# MAGIC %md
# MAGIC ## Further reading
# MAGIC [Snowflake Schema vs Star Schema](https://www.geeksforgeeks.org/difference-between-star-schema-and-snowflake-schema/)
