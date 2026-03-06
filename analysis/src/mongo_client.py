from pymongo import MongoClient

def get_db():
    client = MongoClient("mongodb://localhost:27017")
    db = client["log_pipeline_db"]
    return db