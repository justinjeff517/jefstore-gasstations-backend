import os
import json
from pymongo import MongoClient
from bson import json_util as bsonju

def _err(reason, message, status=400):
    return {"body": json.dumps({"ok": False, "reason": reason, "message": message}), "statusCode": status}

def main(args):
    try:
        uri = os.getenv("MONGODB_URI")
        database_name = os.getenv("MONGODB_DATABASE")
        collection_name = "sales_invoices"

        if not uri or not database_name:
            return _err("config_error", "Missing MONGODB_URI or MONGODB_DATABASE")

        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        col = client[database_name][collection_name]

        # Only consider non-empty forms
        latest = col.find_one(
            {"is_empty": False, "created": {"$exists": True}},
            sort=[("created", -1)]
        )
        if not latest:
            latest = col.find_one({"is_empty": False}, sort=[("_id", -1)])

        if not latest:
            return _err("none_found", "No documents found with is_empty=False.", status=404)

        body = bsonju.dumps({"ok": True, "data": latest})
        return {"body": body, "statusCode": 200}

    except Exception as e:
        return _err("exception", f"{type(e).__name__}: {e}", status=500)
