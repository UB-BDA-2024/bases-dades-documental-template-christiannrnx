from pymongo import MongoClient

class MongoDBClient:
    def __init__(self, host="localhost", port=27017):
        self.host = host
        self.port = port
        self.client = MongoClient(host, port)
        self.database = None
        self.collection = None

    def close(self):
        self.client.close()
    
    def ping(self):
        return self.client.db_name.command('ping')
    
    def getDatabase(self, database):
        self.database = self.client[database]
        return self.database

    def getCollection(self, collection):
        self.collection = self.database[collection]
        return self.collection
    
    def clearDb(self,database):
        self.client.drop_database(database)

    def insertDoc(self, database, collection, document):
        db = self.getDatabase(self, database)
        col = self.getCollection(self,collection)
        doc = col.insert_one(document)
        return doc

    def findDoc(self, database, collection):
        db = self.getDatabase(self, database)
        col = self.getCollection(self,collection)
        doc = col.find_one()
        return doc


