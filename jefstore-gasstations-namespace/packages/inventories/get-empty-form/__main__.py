# Serverless function: returns the single inventories doc where is_empty=True
# Response is always in the "body" field (stringified JSON).

from pymongo import MongoClient
from bson.json_util import dumps
import os

def main(params=None):
    try:
 
        uri = os.getenv("MONGODB_URI")
        database = os.getenv("MONGODB_DATABASE")
        if not uri or not database:
            return {"body": dumps({"ok": False, "error": "Missing MONGODB_URI or MONGODB_DATABASE"})}

        client = MongoClient(uri)
        coll = client[database]["inventories"]

        query = {"is_empty": True}
        count = coll.count_documents(query)

        if count != 1:
            return {"body": dumps({"ok": False, "error": f"Expected exactly 1 is_empty=True document, found {count}"})}

        doc = coll.find_one(query, {"_id": 0})
        return {"body": dumps({"ok": True, "data": doc})}

    except Exception as e:
        return {"body": dumps({"ok": False, "error": str(e)})}
