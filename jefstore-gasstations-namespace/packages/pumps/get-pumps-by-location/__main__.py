import os, json
from pymongo import MongoClient

def main(args):

    client = MongoClient(os.getenv("MONGODB_URI"), tz_aware=True)
    db = client[os.getenv("MONGODB_DATABASE")]
    dispensers = db["dispensers"]

    # parameter (default to "sikatuna" if not given)
    location = args.get("location", "sikatuna")

    pipeline = [
        {"$match": {"location": location}},
        {
            "$lookup": {
                "from": "pumps",
                "let": {"did": "$id", "loc": "$location"},
                "pipeline": [
                    {"$match": {
                        "$expr": {
                            "$and": [
                                {"$eq": [{"$toString": "$dispenser_id"}, {"$toString": "$$did"}]},
                                {"$eq": ["$location", "$$loc"]}
                            ]
                        }
                    }},
                    {"$project": {"_id": 0, "id": 1, "name": 1}}
                ],
                "as": "pumps"
            }
        },
        {"$project": {"_id": 0, "id": 1, "name": 1, "pumps": 1}},
        {"$sort": {"name": 1}}
    ]

    docs = list(dispensers.aggregate(pipeline))

    return {
        "status": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(docs, ensure_ascii=False)
    }
