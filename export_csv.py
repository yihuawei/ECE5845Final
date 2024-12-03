import os
import sys
import xmltodict
import dotenv
from xml.parsers.expat import ExpatError
from neo4j import GraphDatabase
import csv

def ParseFilename(file_name):
    """Extract metadata from the filename."""
    parts = file_name.split('.')
    return {
        "blogger_id": parts[0],
        "gender": parts[1],
        "age": int(parts[2]),
        "industry": parts[3],
        "sign": parts[4] if len(parts) > 4 else None
    }

def CleanXml(xml_data):
    """Clean XML data to handle undefined entities."""
    return xml_data.replace('&', '&amp;')  # Get rid of the escape characters

def ReadFile(file_path):
    """Process an XML file and insert its data into MongoDB."""
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
            xml_data = CleanXml(file.read())
            parsed_data = xmltodict.parse(xml_data)  # Convert XML to dictionary
            
            blog_content = parsed_data.get("Blog", {})
            posts = []
            if "date" in blog_content and "post" in blog_content:
                if(type(blog_content["date"])==list):
                    assert(len(blog_content["date"])==len(blog_content["post"]))
                    for date, post in zip(blog_content["date"], blog_content["post"]):
                        posts.append({"date": date, "content": post})
                else:
                    posts.append({"date": blog_content["date"], "content": blog_content["post"]})
                    # print(blog_content)
                    # exit(0)
            # elif isinstance(blog_content.get("date"), list):
            #     # print(blog_content)
            #     # exit(0)
            #     for date, post in zip(blog_content["date"], blog_content["post"]):
            #         if(type(post)==list):
            #             print("List!!")
            #         posts.append({"date": date, "content": post})
            
            metadata = ParseFilename(os.path.basename(file_path))
            metadata["posts"] = posts
            return metadata
            # collection.insert_one(metadata)
            # print(f"Inserted data from {file_path} successfully.")
    except ExpatError as e:
        print(f"Skipping malformed XML in {file_path}: {e}")
    except Exception as e:
        print(f"Error processing {file_path}: {e}")

def ReadXMLs(xml_directory):
    # Process all .xml files in the directory
    res = []
    for file_name in os.listdir(xml_directory):
        if file_name.endswith(".xml"):
            file_path = os.path.join(xml_directory, file_name)
            this_res = ReadFile(file_path)
            if this_res!=None:
                res.append(this_res)
    return res

if __name__ == '__main__':
    csv_data = ReadXMLs(sys.argv[1])

    f1 = open('user_data.csv', 'w', encoding='utf-8', newline="")
    f2 = open('content_data.csv', 'w', encoding='utf-8', newline="")
    csv1_write = csv.writer(f1)
    csv2_write = csv.writer(f2)
    csv1_write.writerow(['BloggerID', 'Gender', 'Age', 'Industry', 'Sign'])
    csv2_write.writerow(['BloggerID', 'Date', 'Content'])
    
    for data in csv_data:
        date_list = ""
        content_list = ""
        for post in data['posts']:
            if not isinstance(post["date"], type(None)) and not isinstance(post["content"], type(None)):
                assert(type(post["date"])==str)
                assert(type(post["content"])==str)
                csv2_write.writerow([data['blogger_id'], post["date"], post["content"]])

                # date_list = post["date"]
                # content_list = post["content"]

        csv1_write.writerow([data['blogger_id'], data['gender'], data['age'], data['industry'], data['sign']])


    f1.close()
    f2.close()

# LOAD CSV WITH HEADERS FROM "file:///data.csv" AS row 
# MERGE (b:BloggerID {id: toInteger(row.BloggerID)}) WITH b, row
# UNWIND split(row.Date, '|') AS dates
# MERGE (d:Date {date: dates})