
import pandas as pd
df = pd.read_csv('./data.csv')
print(df.to_string()) 

# TODO: Remove whitespace from strings. 
# TODO: Correct inconsistent capitalizations (e.g., product names or cities)
# TODO: Handle invalid or impossible values (e.g., Evan's age of 790)
# TODO: Remove special characters or convert formats (e.g., £ in price column)
# TODO: Handle missing values
# TODO: split out names into first and last names
# TODO: duplicate rows (would need to add timestamps)