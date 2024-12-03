import os
import sys
import xmltodict
import dotenv
from xml.parsers.expat import ExpatError
from neo4j import GraphDatabase

load_status = dotenv.load_dotenv("Neo4j-a3bb9820-Created-2024-11-30.txt")
if load_status is False:
    raise RuntimeError('Environment variables not loaded.')

URI = os.getenv("NEO4J_URI")
AUTH = (os.getenv("NEO4J_USERNAME"), os.getenv("NEO4J_PASSWORD"))


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

blogger_set = set()
gender_set = set()
age_set = set()
industry_set = set()
sign_set = set()

date_set = set()
post_list = []

def ReadFile(file_path):
    """Process an XML file and insert its data into MongoDB."""
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
            xml_data = CleanXml(file.read())
            parsed_data = xmltodict.parse(xml_data)  # Convert XML to dictionary
            
            blog_content = parsed_data.get("Blog", {})
            posts = []
            if "date" in blog_content and "post" in blog_content:
                # posts = {"date": blog_content["date"], "content": blog_content["post"]}
                posts.append({"date": blog_content["date"], "content": blog_content["post"]})
            elif isinstance(blog_content.get("date"), list):
                for date, post in zip(blog_content["date"], blog_content["post"]):
                    posts.append({"date": date, "content": post})
            # if(len(posts)>1):
            #     print(len(posts))

            metadata = ParseFilename(os.path.basename(file_path))
            blogger_set.add(metadata["blogger_id"])
            gender_set.add(metadata["gender"])
            age_set.add(metadata["age"])
            industry_set.add(metadata["industry"])
            sign_set.add(metadata["sign"])

            for post in posts:
                for d in post["date"]:
                    date_set.add(d)
                if type(post["content"])==list:
                    for c in post["content"]:
                        post_list.append(c)
                elif type(post["content"])==str:
                    post_list.append(post["content"])


            # metadata["posts"] = posts

            # collection.insert_one(metadata)
            # print(f"Inserted data from {file_path} successfully.")
    except ExpatError as e:
        pass
        # print(xml_data)
        # print(f"Skipping malformed XML in {file_path}: {e}")
    except Exception as e:
        print(f"Error processing {file_path}: {e}")

def ReadXMLs(xml_directory):
    # Process all .xml files in the directory
    for file_name in os.listdir(xml_directory):
        if file_name.endswith(".xml"):
            file_path = os.path.join(xml_directory, file_name)
            ReadFile(file_path)


if __name__ == '__main__':
    ReadXMLs(sys.argv[1])
    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        driver.verify_connectivity()
        print("Connection established.")
    try:
        for id in blogger_set:
            records, summary, keys = driver.execute_query(
                "Create (b:Blogger  {id: $id})",
                id = id,
                database_="neo4j"
            )
    except Exception as e:
        print(e)
    print(date_set)

