# Serverless function: returns the latest inventories doc where is_empty=True
# Response is always in the "body" field (stringified JSON).

from pymongo import MongoClient, DESCENDING
from bson.json_util import dumps
import os

def main(params=None):
    try:
        uri = os.getenv("MONGODB_URI")
        database = os.getenv("MONGODB_DATABASE")
        if not uri or not database:
            return {"body": dumps({"ok": False, "error": "Missing MONGODB_URI or MONGODB_DATABASE"})}

        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        coll = client[database]["inventories"]

        # Pick the most recently created empty form; if ties, fall back to business date and QR.
        query = {"is_empty": True}
        projection = {"_id": 0}
        sort = [("created", DESCENDING), ("date", DESCENDING), ("current_form_qr_code", DESCENDING)]

        doc = coll.find_one(query, projection=projection, sort=sort)
        if not doc:
            return {"body": dumps({"ok": False, "error": "No is_empty=True document found"})}

        return {"body": dumps({"ok": True, "data": doc})}

    except Exception as e:
        return {"body": dumps({"ok": False, "error": str(e)})}
a