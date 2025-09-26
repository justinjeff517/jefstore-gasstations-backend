import os
import json
import re
from pymongo import MongoClient

def main(args):
    try:
        location = (args.get("location") or "").strip()
        if not location:
            return {
                "statusCode": 400,
                "headers": {"content-type": "application/json"},
                "body": json.dumps({"ok": False, "error": "Missing required parameter: location"})
            }

        uri = os.getenv("MONGODB_URI")
        db_name = os.getenv("MONGODB_DATABASE")
        if not uri or not db_name:
            return {
                "statusCode": 500,
                "headers": {"content-type": "application/json"},
                "body": json.dumps({"ok": False, "error": "Missing MONGODB_URI or MONGODB_DATABASE"})
            }

        client = MongoClient(uri, connectTimeoutMS=5000, serverSelectionTimeoutMS=5000)
        db = client[db_name]
        coll = db["dispensers"]

        q = {"location": {"$regex": f"^{re.escape(location)}$", "$options": "i"}}
        docs = list(coll.find(q, {"_id": 0, "id": 1, "name": 1, "location": 1}).sort([("name", 1)]))

        return {
            "statusCode": 200,
            "headers": {"content-type": "application/json"},
            "body": json.dumps(docs)
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "headers": {"content-type": "application/json"},
            "body": json.dumps({"ok": False, "error": str(e)})
        }
