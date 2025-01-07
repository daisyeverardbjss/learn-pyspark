from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("pyspark tutorial").getOrCreate()

df = spark.read.csv("./data.csv")
df.show()

# # TODO: Remove whitespace from strings. 
# df['name'] = df['name'].str.strip()
# df['city'] = df['city'].str.strip()
# df['product'] = df['product'].str.strip()
# df['price'] = df['price'].str.strip()

# # TODO: remove pound symbols and make sure the price column has the right type
# df['price'] = df['price'].str.strip('£')
# df = df.astype({'price': 'float'})
# print(df.dtypes)

# # TODO: Correct inconsistent capitalizations on products and cities. Use Title Case
# df['product'] = df['product'].str.title()
# df['city'] = df['city'].str.title()

# # TODO: Handle NaNs / empty cells. Maybe only for certain columns. e.g. don't drop if Nan in city
# df = df.dropna()

# # TODO: remove impossible values (Age should be no higher than 100)
# df = df[df.age <= 100]

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