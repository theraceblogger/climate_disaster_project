import pandas as pd

df = pd.read_csv('~/weather-db-example/station.csv')
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
print(df.head())

