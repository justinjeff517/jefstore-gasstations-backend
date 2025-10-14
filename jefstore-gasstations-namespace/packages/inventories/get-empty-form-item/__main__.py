import os
from pymongo import MongoClient

from bson import json_util as bsonju

def main(args):

    barcode = args.get("barcode")
    if not barcode:
        return {"statusCode": 400, "body": "Missing parameter: barcode"}

    try:
        client = MongoClient(os.getenv("MONGODB_URI"))
        coll = client[os.getenv("MONGODB_DATABASE")]["inventories"]

        # Find the open (is_empty=True) form containing the given barcode
        doc = coll.find_one(
            {"is_empty": True, "items.barcode": barcode},
            {"_id": 0, "id": 1, "current_form_qr_code": 1, "is_empty": 1, "items": 1}
        )

        if not doc:
            return {"statusCode": 404, "body": f"No open inventory contains barcode {barcode}"}

        # Extract matching item
        items = doc.get("items") or []
        item = next((it for it in items if it.get("barcode") == barcode), None)
        if not item:
            return {"statusCode": 404, "body": f"Item with barcode {barcode} not found"}

        # Prepare response
        out = {
            "barcode": item.get("barcode"),
            "type": item.get("type"),
            "name": item.get("name"),
            "price": item.get("price"),
            "unit": item.get("unit"),
            "previous_quantity": item.get("previous_quantity"),
            "addstock": item.get("addstock"),
            "sold": item.get("sold"),
            "current_quantity": item.get("current_quantity"),
            "created": item.get("created"),
            "updated": item.get("updated"),
            "id": item.get("id"),
            "item_number": item.get("item_number"),
        }

        return {
            "statusCode": 200,
            "body": bsonju.dumps(out, ensure_ascii=False)
        }

    except Exception as e:
        return {"statusCode": 500, "body": str(e)}
