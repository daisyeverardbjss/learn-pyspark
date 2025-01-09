
import pandas as pd

# TODO: read in a csv with the correct column headers
df = pd.read_csv('./data.csv')
print(df.to_string()) 

# look at the data


# TODO: Remove whitespace from strings. (look at trim)
# TODO: Correct inconsistent capitalizations (e.g., product names or cities) (look at regex_replace)
# TODO: Handle invalid or impossible values (e.g., Evan's age of 790)
# TODO: Remove special characters or convert formats (e.g., £ in price column)
# TODO: Handle missing values
# TODO: split out names into first and last names