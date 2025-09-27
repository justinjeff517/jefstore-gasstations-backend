import os, json
from pymongo import MongoClient
from bson.decimal128 import Decimal128
from datetime import datetime, timezone

def _enc(o):
    if isinstance(o, Decimal128):
        return str(o.to_decimal())
    if isinstance(o, datetime):
        return o.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return str(o)

def _pipeline(location: str):
    return [
        {"$match": {"location": location}},
        {
            "$lookup": {
                "from": "pumps",
                "let": {"did": "$id", "loc": "$location"},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": [{"$toString": "$dispenser_id"}, {"$toString": "$$did"}]},
                                    {"$eq": ["$location", "$$loc"]}
                                ]
                            }
                        }
                    },
                    {"$project": {"_id": 0, "id": 1, "name": 1}}
                ],
                "as": "pumps"
            }
        },
        {"$unwind": {"path": "$pumps", "preserveNullAndEmptyArrays": False}},
        {
            "$lookup": {
                "from": "pump_inventories",
                "let": {"pid": {"$toString": "$pumps.id"}, "loc": "$location"},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": [{"$toString": "$pump_id"}, "$$pid"]},
                                    {"$eq": ["$location", "$$loc"]}
                                ]
                            }
                        }
                    },
                    {"$sort": {"date": -1, "created_at": -1}},
                    {"$limit": 1},
                    {"$project": {"_id": 0}}
                ],
                "as": "latest"
            }
        },
        {"$addFields": {"pumps.latest_inventory": {"$arrayElemAt": ["$latest", 0]}}},
        {"$project": {"latest": 0}},
        {
            "$group": {
                "_id": "$id",
                "id": {"$first": "$id"},
                "name": {"$first": "$name"},
                "location": {"$first": "$location"},
                "pumps": {"$push": "$pumps"}
            }
        },
        {"$project": {"_id": 0, "id": 1, "name": 1, "location": 1, "pumps": 1}},
        {"$sort": {"name": 1}}
    ]

def main(args):
    try:
        location = str(args.get("location", "loboc"))
        mongo_uri = os.getenv("MONGODB_URI")
        db_name = os.getenv("MONGODB_DATABASE")

        client = MongoClient(mongo_uri, tz_aware=True)
        db = client[db_name]
        dispensers = db["dispensers"]

        docs = list(dispensers.aggregate(_pipeline(location), allowDiskUse=True))

        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type",
                "Access-Control-Allow-Methods": "GET, OPTIONS"
            },
            "body": json.dumps(docs, ensure_ascii=False, default=_enc)
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*"
            },
            "body": json.dumps({"error": str(e)})
        }
