import os
import sys
import xmltodict
import dotenv
from xml.parsers.expat import ExpatError
from neo4j import GraphDatabase
import csv
import pandas as pd


load_status = dotenv.load_dotenv("Neo4j-a3bb9820-Created-2024-11-30.txt")
if load_status is False:
    raise RuntimeError('Environment variables not loaded.')

# load_movies_csv = """
#     LOAD CSV FROM 'https://github.com/yihuawei/ECE5845Final/blob/main/data.csv' AS row 
#     MERGE (b:BloggerID2 {id: 10})
# """

URI = os.getenv("NEO4J_URI")
AUTH = (os.getenv("NEO4J_USERNAME"), os.getenv("NEO4J_PASSWORD"))

def write_user_data(tx, batch):
    query = """
        UNWIND $batch AS data
        Create (b:Blogger {id: toInteger(data[0]), gender: data[1], age: toInteger(data[2]), industry: data[3], sign: data[4]})
    """
    tx.run(query, batch=batch)

def write_content_data(tx, batch):
    query = """
        UNWIND $batch AS data
        MERGE (b:Blogger {id: toInteger(data[0])}) WITH b, data
        Create (p:Post {text: data[2], time: data[1]}) WITH b, p, data
        Create (b)-[:POST]->(p)
    """
    tx.run(query, batch=batch)


with GraphDatabase.driver(URI, auth=AUTH) as driver:
    driver.verify_connectivity()
    print("Connection established.")

    df1 = pd.read_csv('user_data.csv')
    df2 = pd.read_csv('content_data.csv')

    with driver.session(database="neo4j") as session:
        batch_size = 5000
        # data = df1.values.tolist()
        # for i in range(0, len(data), batch_size):
        #     print(i, "out of", len(data))
        #     batch = data[i:i + batch_size]
        #     session.execute_write(write_user_data, batch)
        
        content = df2.values.tolist()
        for i in range(0, len(content), batch_size):
            print(i, "out of", len(content))
            batch = content[i:i + batch_size]
            for k in batch:
                assert(len(k)==3)
            # print(batch)
            session.execute_write(write_content_data, batch)

            # print(row[1][0])
            # session.run("""
            #     Create (b:Blogger  {id: $id})
            # """, id = row[1][0])
        
#usecols=['BloggerID', 'Gender']


# with open('content_data.csv', mode ='r')as file:
#   csvFile = csv.reader(file)
#   for lines in csvFile:
#         print(lines)