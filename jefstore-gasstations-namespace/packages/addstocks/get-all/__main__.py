import os, json
from pymongo import MongoClient
from bson import json_util

MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
MONGODB_DATABASE = os.getenv("MONGODB_DATABASE", "test")
client = MongoClient(MONGODB_URI)
db = client[MONGODB_DATABASE]
coll = db["addstocks"]

def main(args):
    try:
        docs = list(coll.find().sort("date", 1))
        body = json.dumps(
            {"ok": True, "count": len(docs), "items": docs},
            default=json_util.default,
            ensure_ascii=False
        )
        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json; charset=utf-8",
                "Access-Control-Allow-Origin": "*"
            },
            "body": body
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "headers": {
                "Content-Type": "application/json; charset=utf-8",
                "Access-Control-Allow-Origin": "*"
            },
            "body": json.dumps({"ok": False, "error": str(e)}, ensure_ascii=False)
        }
