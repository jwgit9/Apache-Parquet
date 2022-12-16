import pandas as pd
import os

data = [['apple', 10], ['orange', 15], ['peach', 14]]

df = pd.DataFrame(data, columns=['Fruit', 'Amount'])

df.to_parquet("dataframe.parquet", engine="pyarrow")  # pip install pyarrow

print("Listing the contents of the current directory:")
print(os.listdir('.'))

df2 = pd.read_parquet("dataframe.parquet")
df.to_csv("filename.csv")
