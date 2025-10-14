from pymongo import MongoClient, DESCENDING
from bson import json_util as bsonju
import os

def main(params=None):
    try:
        uri = os.getenv("MONGODB_URI")
        database = os.getenv("MONGODB_DATABASE")
        if not uri or not database:
            return {"body": bsonju.dumps({"ok": False, "error": "Missing MONGODB_URI or MONGODB_DATABASE"})}

        with MongoClient(uri, serverSelectionTimeoutMS=5000) as client:
            client.admin.command("ping")
            coll = client[database]["inventories"]

            query = {"is_empty": False}
            projection = {"_id": 0}
            sort = [("date", DESCENDING)]

            doc = coll.find_one(query, projection=projection, sort=sort)
            if not doc:
                return {"body": bsonju.dumps({"ok": False, "error": "No is_empty=False document found"})}

            return {"body": bsonju.dumps({"ok": True, "data": doc})}

    except Exception as e:
        return {"body": bsonju.dumps({"ok": False, "error": str(e)})}
