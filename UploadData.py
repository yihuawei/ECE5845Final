import os
import sys
import xmltodict
import dotenv
import csv
from xml.parsers.expat import ExpatError
from neo4j import GraphDatabase


class XMLReader:
    def __init__(self, path, default_num_post = -1):
        assert(os.path.isdir(path))
        self.json_data_ = []
        self.meta_data_ = []
        self.content_data_ = []
        self.default_num_post_ = default_num_post
        self.ReadDirectory(path)
    
    def meta_data(self):
        return self.meta_data_
    def content_data(self):
        return self.content_data_

    def _parse_filename(self, file_name):
        """Extract metadata from the filename."""
        parts = file_name.split('.')
        return {
            "blogger_id": parts[0],
            "gender": parts[1],
            "age": int(parts[2]),
            "industry": parts[3],
            "sign": parts[4] if len(parts) > 4 else None
        }

    def _clean_xml_data(self, xml_data):
        """Clean XML data to handle undefined entities."""
        return xml_data.replace('&', '&amp;')  # Get rid of the escape characters 

    def ReadFile(self, file_path):
        """Process an XML file and insert its data into MongoDB."""
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
                xml_data = self._clean_xml_data(file.read())
                parsed_data = xmltodict.parse(xml_data)  # Convert XML to dictionary
                
                blog_content = parsed_data.get("Blog", {})
                posts = []
                if "date" in blog_content and "post" in blog_content:
                    if(type(blog_content["date"])==list):
                        assert(len(blog_content["date"])==len(blog_content["post"]))
                        num_post = 0
                        for date, post in zip(blog_content["date"], blog_content["post"]):
                            posts.append({"date": date, "content": post})
                            num_post+=1
                            if num_post>=self.default_num_post_:
                                break    
                    else:
                        posts.append({"date": blog_content["date"], "content": blog_content["post"]})
                metadata = self._parse_filename(os.path.basename(file_path))
                metadata["posts"] = posts
                # print(f"Inserted data from {file_path} successfully.")
                return metadata
        except ExpatError as e:
            pass
            #print(f"Skipping malformed XML in {file_path}: {e}")
        except Exception as e:
            pass
            #print(f"Error processing {file_path}: {e}")

    def ReadDirectory(self, xml_dir):
        # Process all .xml files in the directory
        counter = 0
        for file_name in os.listdir(xml_dir):
            if counter%100==0:
                print(f"    {counter} out of {len(os.listdir(xml_dir))} XML files have been processed\r", end='', flush=True)
            
            if file_name.endswith(".xml"):
                this_json = self.ReadFile(os.path.join(xml_dir, file_name))
                if this_json!=None:
                    counter+=1
                    self.json_data_.append(this_json)
        
        for data in self.json_data_:
            self.meta_data_.append([data['blogger_id'], data['gender'], data['age'], data['industry'], data['sign']])
            for post in data['posts']:
                if not isinstance(post["date"], type(None)) and not isinstance(post["content"], type(None)):
                    assert(type(post["date"])==str)
                    assert(type(post["content"])==str)
                    self.content_data_.append([data['blogger_id'], post["date"], post["content"]])
        
        print(f"{counter} out of {len(os.listdir(xml_dir))} XML files have been processed")
        print(f"Length of Meta Data List {len(self.meta_data_)}")
        print(f"Length of Content Data List {len(self.content_data_)}")

class Neo4jUploader:
    def __init__(self, key_file, uploader):
        if dotenv.load_dotenv(key_file) is False:
            raise RuntimeError('Environment variables not loaded.')
        #self.URI = os.getenv("NEO4J_URI")
        #self.AUTH = (os.getenv("NEO4J_USERNAME"), os.getenv("NEO4J_PASSWORD"))
        self.URI = "neo4j://localhost"
        self.AUTH = ("neo4j", "")
        self.meta_data_ = uploader.meta_data()
        self.content_data_ = uploader.content_data()
        self.Upload()

    
    def _write_user_data(self, tx, batch):
        # query = """
        #     UNWIND $batch AS data
        #     Create (b:Blogger {id: toInteger(data[0]), gender: data[1], age: toInteger(data[2]), industry: data[3], sign: data[4]})
        # """
        query = """
            UNWIND $batch AS data
            Create (b:Blogger {id: toInteger(data[0])})
            Merge (g:Gender {gender: data[1]})
            Merge (a:Age {age: toInteger(data[2])})
            Merge (i:Industry {industry: data[3]})
            Merge (s:Sign {sign: data[4]});
        """
        tx.run(query, batch=batch)
        
        query = """
            UNWIND $batch AS data
            Match (b:Blogger {id: toInteger(data[0])}), (g:Gender {gender: data[1]})
            Create (b)-[:has_gender]->(g);
        """
        tx.run(query, batch=batch)
        
        query = """
            UNWIND $batch AS data
            Match (b:Blogger {id: toInteger(data[0])}), (a:Age {age: toInteger(data[2])})
            Create (b)-[:has_age]->(a);
        """
        tx.run(query, batch=batch)

        query = """
            UNWIND $batch AS data
            Match (b:Blogger {id: toInteger(data[0])}), (i:Industry {industry: data[3]})
            Create (b)-[:has_industry]->(i);
        """
        tx.run(query, batch=batch)

        query = """
            UNWIND $batch AS data
            Match (b:Blogger {id: toInteger(data[0])}), (s:Sign {sign: data[4]})
            Create (b)-[:has_sign]->(s);
        """
        tx.run(query, batch=batch)


    def _write_content_data(self, tx, batch):
        query = """
            UNWIND $batch AS data
            MERGE (b:Blogger {id: toInteger(data[0])}) WITH b, data
            Create (p:Post {text: data[2], time: data[1]}) WITH b, p, data
            Create (b)-[:POST]->(p)
        """
        tx.run(query, batch=batch)

    def Upload(self):
        with GraphDatabase.driver(self.URI, auth=self.AUTH) as driver:
            driver.verify_connectivity()
            print("Neo4j Connection established.")
            with driver.session(database="neo4j") as session:
                batch_size = 100
                for i in range(0, len(self.meta_data_), batch_size):
                    batch = self.meta_data_[i:i + batch_size]
                    session.execute_write(self._write_user_data, batch)
                    print(f"{i} out of {len(self.meta_data_)} meta data have been uploaded to Neo4j server\r", end='', flush=True)
                
                # for i in range(0, len(self.content_data_), batch_size):
                #     batch = self.content_data_[i:i + batch_size]
                #     for k in batch:
                #         assert(len(k)==3)
                #     print(f"{i} out of {len(self.content_data_)} content data have been uploaded to Neo4j server\r", end='', flush=True)
                #     session.execute_write(self._write_content_data, batch)     


if __name__ == '__main__':
    key_file = "Neo4j-a3bb9820-Created-2024-11-30.txt"
    neo4j_loader = Neo4jUploader(key_file, XMLReader(sys.argv[1], 2))
