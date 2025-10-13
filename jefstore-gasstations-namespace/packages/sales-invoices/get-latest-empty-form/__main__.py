# DigitalOcean Functions (Python) — Get latest sales invoice by `created` date
# - Uses env: MONGODB_URI, MONGODB_DATABASE
# - Returns JSON via "body"

import os, json
from pymongo import MongoClient

_client = None
_col = None

def _get_collection():
    global _client, _col
    if _col is not None:
        return _col


    uri = os.getenv("MONGODB_URI")
    database_name = os.getenv("MONGODB_DATABASE")
    if not uri or not database_name:
        raise RuntimeError("Missing MONGODB_URI or MONGODB_DATABASE")

    _client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    db = _client[database_name]
    _col = db["sales_invoices"]

    try:
        _col.create_index([("created", -1)], background=True, name="idx_created_desc")
    except Exception:
        pass

    return _col

def _resp(status=200, payload=None):
    if payload is None:
        payload = {}
    return {
        "statusCode": status,
        "headers": {"content-type": "application/json"},
        "body": json.dumps(payload, default=str),
    }

def main(args):
    try:
        col = _get_collection()
        latest = col.find_one({}, sort=[("created", -1), ("_id", -1)])
        if not latest:
            return _resp(404, {"ok": False, "error": "No documents found in collection."})

        return _resp(200, {"ok": True, "data": latest})
    except Exception as e:
        return _resp(500, {"ok": False, "error": str(e)})
