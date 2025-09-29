import os, json
from pymongo import MongoClient

client = MongoClient(os.getenv("MONGODB_URI"))
db = client[os.getenv("MONGODB_DATABASE")]
coll = db["pump_inventories"]

def resp(code, obj):
    return {
        "statusCode": code,
        "headers": {"content-type": "application/json"},
        "body": json.dumps(obj, default=str),
    }

def main(args):
    try:
        location = args.get("location")
        pump_id = args.get("pump_id")
        if not location or not pump_id:
            return resp(400, {"ok": False, "error": "location and pump_id are required"})

        doc = coll.find_one(
            {"location": location, "pump_id": str(pump_id)},
            {"_id": 0},
            sort=[("date", -1), ("created_at", -1)],
        )
        if not doc:
            return resp(404, {"ok": False, "error": "not found"})

        return resp(200, doc)
    except Exception as e:
        return resp(500, {"ok": False, "error": str(e)})
