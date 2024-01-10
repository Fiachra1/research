import pandas as pd
from pymongo import MongoClient
import json


def mongoimport(csv_path, db_name, coll_name, db_url="localhost", db_port=27000):
    """Imports a csv file at path csv_name to a mongo collection
    returns: count of the documants in the new collection
    """
    client = MongoClient(db_url, db_port)
    db = client[db_name]
    collection = db[coll_name]
    data = pd.read_csv(csv_path)
    payload = json.loads(data.to_json(orient="records"))
    #   collection.remove()
    collection.insert(payload)
    print("success")
    return collection.count()


path = "/Users/fmerwick/Downloads/test.csv"

db = mongoimport(path, "test_db", "test_collection")
print(db)
