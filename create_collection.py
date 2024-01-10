import pandas as pd
import json
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

uri = "mongodb+srv://fmerwick:Golfaholic27!@fm-research.kcebon0.mongodb.net/?retryWrites=true&w=majority"

# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi("1"))

# Send a ping to confirm a successful connection
try:
    client.admin.command("ping")
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)

db = client["research"]
collection = db["papers"]


def mongoimport(csv_path, collection):
    """Imports a csv file at path csv_name to a mongo collection
    returns: count of the documants in the new collection
    """
    data = pd.read_csv(csv_path)
    payload = json.loads(data.to_json(orient="records"))
    collection.delete_many({})
    collection.insert_many(payload)
    print("success")
    return collection.count_documents({})


path = "/Users/fmerwick/Downloads/test.csv"

db = mongoimport(path, collection)
print(db)
