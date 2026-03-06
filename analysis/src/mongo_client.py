import os

try:
    from pymongo import MongoClient
except ModuleNotFoundError:
    MongoClient = None


MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("LOG_PIPELINE_DB_NAME", "log_pipeline")


def get_db():
    if MongoClient is None:
        raise RuntimeError("pymongo is not installed")
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
    return client[DB_NAME]
