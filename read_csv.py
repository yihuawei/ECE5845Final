import csv
import pandas as pd

# with open('data.csv', encoding='utf-8') as csvfile:
#     reader_obj = csv.DictReader(csvfile)
#     # print(reader_obj)
#     for row in reader_obj:  # 以list形式返回一行
#         print(row['BloggerID'])

df = pd.read_csv('data.csv', usecols=['Content'])
    # print(df)
for row in df.iterrows():
    print(row[1])