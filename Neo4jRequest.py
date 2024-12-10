
# import os
# import sys
# import dotenv
# from neo4j import GraphDatabase
#from graphdatascience import GraphDataScience

class Neo4jBloggerReq:
    # def _QueryBloogers(self, tx, key, val, res_key):
    #     # query = f"MATCH (b:Blogger {{id}: val}) RETURN b"
        
    #     res = tx.run("""
    #         MATCH (b:Blogger {id: $val})
    #         RETURN b""", 
    #         key = key,
    #         val = val)
    #     return [r.data()['p.text'] for r in list(res)]

    # def QueryBloogers(self, key, val, res_key=None) -> set:

    #     with self.driver_.session(database="neo4j") as session:
    #         res = session.execute_read(self._QueryBloogers, key, val, res_key)
    #         return res

    def __init__(self, key_file=None):
        pass
        # if key_file==None:
        #     if dotenv.load_dotenv(key_file) is False:
        #         raise RuntimeError('Environment variables not loaded.')
        #     self.URI = os.getenv("NEO4J_URI")
        #     self.AUTH = (os.getenv("NEO4J_USERNAME"), os.getenv("NEO4J_PASSWORD"))
        # else:
        #     self.URI = "neo4j://localhost"
        #     self.AUTH = ("neo4j", "")
        # self.driver_ =  GraphDatabase.driver(self.URI, auth=self.AUTH) 
        # self.driver_.verify_connectivity()
        
        # self.gds_ = GraphDataScience(self.AUTH, auth=self.AUTH)
        # assert(self.gds_.version())
        # print("Neo4j Connection established.")



    # def _GetPostsByBloggerID(self, tx, blogger_id):
    #     res = tx.run("""
    #         MATCH (b:Blogger {id: $blogger_id})-[:POST]-(p:Post)
    #         RETURN p.text""", 
    #         blogger_id = blogger_id)
    #     return [r.data()['p.text'] for r in list(res)]

    # def GetPostsByBloggerID(self, blogger_id) -> list:
    #      with self.driver_.session(database="neo4j") as session:
    #         res = session.execute_read(self._GetPostsByBloggerID, blogger_id)
    #         return res
    
    # def _GetPostsByGender(self, tx, gender):
    #     res = tx.run("""
    #         MATCH (b:Blogger {gender: $gender})-[:POST]-(p:Post)
    #         RETURN p.text""", 
    #         gender = gender)
    #     return [r.data()['p.text'] for r in list(res)]

    # def GetPostsByGender(self, gender) -> list:
    #      with self.driver_.session(database="neo4j") as session:
    #         res = session.execute_read(self._GetPostsByGender, gender)
    #         return res

    # def _GetBloggerSignByGender(self, tx, gender):
    #     res = tx.run("""
    #         MATCH (b:Blogger {gender: $gender})
    #         RETURN b.sign""", 
    #         gender = gender)
    #     return set([r.data()['b.sign'] for r in list(res)])
    
    # def GetBloggerSignByGender(self, gender) -> set:
    #     with self.driver_.session(database="neo4j") as session:
    #         res = session.execute_read(self._GetBloggerByGender, gender)
    #         return res
        

#  {nodeProperties: ['id', 'gender', 'age', 'industry', 'sign']}

# CALL gds.graph.project('node2vec', 
#         {
#          Age: {properties: 'age'},
#          Blogger: {properties: 'id'},
#          Gender: {properties: 'gender'},
#          Industry: {properties: 'industry'},
#          Sign: {properties: 'sign'}
#         },
#         ['has_age', 'has_gender', 'has_industry', 'has_sign'])


# CALL gds.graph.project('node2vec', 
#         ['Age', 'Blogger', 'Gender', 'Industry', 'Sign'],
#         ['has_age', 'has_gender', 'has_industry', 'has_sign'])

# CALL gds.graph.drop('node2vec')


# CALL gds.knn.stream('node2vec', {
#     nodeLabels:['Blogger'],
#     topK: 5,
#     nodeProperties: ['feature'],
#     randomSeed: 1337,
#     concurrency: 1,
#     sampleRate: 1.0,
#     deltaThreshold: 0.0
# })

# CALL gds.node2vec.mutate('node2vec', {nodeLabels:['Blogger'], embeddingDimension: 5, mutateProperty:"feature"})

# CALL gds.node2vec.write('node2vec', {nodeLabels:['Blogger'], embeddingDimension: 5, writeProperty:"feature"})
# YIELD nodeId, embedding
# RETURN nodeId, embedding

if __name__ == '__main__':
    #app = Neo4jBloggerReq("Neo4j-a3bb9820-Created-2024-11-30.txt")
    app = Neo4jBloggerReq()
    # print(app.GetPostsByBloggerID(3846432))
    #print(len(app.GetPostsByGender("male")))
    # print(app.GetBloggerSignByGender("female"))

        
       



