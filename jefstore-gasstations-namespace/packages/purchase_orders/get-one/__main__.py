import os
import json
from pymongo import MongoClient

def main(args):
    # Ensure only GET method is allowed
    if args.get("__ow_method", "").upper() != "GET":
        return {
            "statusCode": 405,
            "headers": {"content-type": "application/json"},
            "body": json.dumps({"error": "Method not allowed, only GET supported"})
        }

    po_number = args.get("po_number")
    if not isinstance(po_number, str) or not po_number.strip():
        return {
            "statusCode": 400,
            "headers": {"content-type": "application/json"},
            "body": json.dumps({"error": "Missing or invalid 'po_number' parameter"})
        }

    # Connect to MongoDB
    client = MongoClient(os.getenv("MONGODB_URI", ""))
    db = client['jef-erp-database']
    collection = db["purchase_orders"]

    # Query by po_number (exclude _id)
    doc = collection.find_one({"po_number": po_number}, {"_id": 0})

    if doc:
        return {
            "statusCode": 200,
            "headers": {"content-type": "application/json"},
            "body": json.dumps(doc)
        }
    else:
        return {
            "statusCode": 404,
            "headers": {"content-type": "application/json"},
            "body": json.dumps({"error": f"No purchase order found with po_number={po_number}"})
        }
