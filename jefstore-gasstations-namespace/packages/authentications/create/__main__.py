import os, json, uuid
from datetime import datetime, timedelta
from pymongo import MongoClient

client = MongoClient(os.getenv("MONGODB_URI"))
db = client[os.getenv("MONGODB_DATABASE")]

def main(args):
    username = args.get("username")
    password = args.get("password")
    if not username or not password:
        return {"body": json.dumps({"message": "missing username or password"})}

    acc = db["accounts"].find_one({"username": username, "password": password}, {"_id": 0})
    if not acc:
        return {"body": json.dumps({"message": "invalid credentials"})}

    now = datetime.utcnow()
    today = datetime(now.year, now.month, now.day)
    tomorrow = today + timedelta(days=1)

    existing = db["auth_logs"].find_one({
        "user_id": acc["id"],
        "timestamp": {"$gte": today.isoformat()+"Z", "$lt": tomorrow.isoformat()+"Z"}
    })

    if existing:
        return {"body": json.dumps({"message": "already logged in"})}

    log = {
        "id": str(uuid.uuid4()),
        "timestamp": now.isoformat()+"Z",
        "user_id": acc["id"],
        "username": acc["username"],
        "is_granted": True
    }
    db["auth_logs"].insert_one(log)

    return {"body": json.dumps({"message": "login recorded"})}
