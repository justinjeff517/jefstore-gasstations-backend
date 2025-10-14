import os
import json
from datetime import datetime, timezone
from pymongo import MongoClient
from bson import json_util as bsonju

def main(args):
    # --- Enforce PATCH ---
    method = (args.get("__ow_method") or "").lower()
    if method and method != "patch":
        return {"body": json.dumps({"ok": False, "error": "Method Not Allowed. Use PATCH."})}

    # --- Required params ---
    barcode = args.get("barcode")
    addstock = args.get("addstock")
    sold = args.get("sold")

    if not barcode:
        return {"body": json.dumps({"ok": False, "error": "Missing parameter: barcode"})}
    if addstock is None or sold is None:
        return {"body": json.dumps({"ok": False, "error": "Missing parameters: addstock and sold are required"})}

    def _num(n):
        if isinstance(n, (int, float)): 
            return float(n)
        try:
            return float(str(n).replace(",", ""))
        except Exception:
            raise ValueError("addstock/sold must be numeric")

    try:
        addstock_f = _num(addstock)
        sold_f = _num(sold)
        if addstock_f < 0 or sold_f < 0:
            return {"body": json.dumps({"ok": False, "error": "addstock and sold must be non-negative"})}
    except ValueError as e:
        return {"body": json.dumps({"ok": False, "error": str(e)})}

    # --- Connect ---
    uri = os.getenv("MONGODB_URI")
    dbname = os.getenv("MONGODB_DATABASE")
    if not uri or not dbname:
        return {"body": json.dumps({"ok": False, "error": "Server misconfigured: missing MONGODB_URI/MONGODB_DATABASE"})}

    client = MongoClient(uri)
    coll = client[dbname]["inventories"]

    # --- Identify the ONLY open form (is_empty=True) ---
    open_forms = list(coll.find({"is_empty": True}, {"_id": 1, "id": 1, "current_form_qr_code": 1}).limit(2))
    if len(open_forms) == 0:
        return {"body": json.dumps({"ok": False, "error": "No inventory document has is_empty=True."})}
    if len(open_forms) > 1:
        return {"body": json.dumps({"ok": False, "error": "More than one inventory document has is_empty=True; cannot determine target uniquely."})}

    target_oid = open_forms[0]["_id"]

    # --- Perform the in-place item update (do NOT touch is_empty) ---
    now_dt = datetime.now(timezone.utc)  # MongoDB date
    pipeline_update = [
        {
            "$set": {
                "items": {
                    "$map": {
                        "input": "$items",
                        "as": "it",
                        "in": {
                            "$cond": [
                                {"$eq": ["$$it.barcode", barcode]},
                                {
                                    "$mergeObjects": [
                                        "$$it",
                                        {
                                            "addstock": addstock_f,
                                            "sold": sold_f,
                                            "updated": now_dt,
                                            "current_quantity": {
                                                "$subtract": [
                                                    {"$add": ["$$it.previous_quantity", addstock_f]},
                                                    sold_f
                                                ]
                                            }
                                        }
                                    ]
                                },
                                "$$it"
                            ]
                        }
                    }
                }
            }
        }
    ]

    res = coll.update_one(
        {"_id": target_oid, "is_empty": True, "items.barcode": barcode},
        pipeline_update
    )

    if res.matched_count == 0:
        return {"body": json.dumps({"ok": False, "error": "Open form not found with the given barcode (or no longer is_empty=True)."})}

    # --- Read back ONLY the updated item efficiently ---
    try:
        agg_res = coll.aggregate([
            {"$match": {"_id": target_oid}},
            {"$project": {
                "_id": 0,
                "id": 1,
                "current_form_qr_code": 1,
                "is_empty": 1,
                "items": {
                    "$filter": {
                        "input": "$items",
                        "as": "it",
                        "cond": {"$eq": ["$$it.barcode", barcode]}
                    }
                }
            }},
            {"$limit": 1}
        ]).next()
    except StopIteration:
        return {"body": json.dumps({"ok": False, "error": "Updated document not found after write (unexpected)."})}

    items = agg_res.get("items") or []
    if not items:
        return {"body": json.dumps({"ok": False, "error": "Updated item not found after write (unexpected)."})}

    item = items[0]

    # --- Balance check with tolerance (handles float precision) ---
    lhs = float(item.get("previous_quantity", 0)) + float(item.get("addstock", 0)) - float(item.get("sold", 0))
    rhs = float(item.get("current_quantity", 0))
    balance_ok = abs(lhs - rhs) < 1e-9

    # --- Response (keep MongoDB dates in Extended JSON) ---
    payload = {
        "ok": True,
        "inventory_id": agg_res.get("id"),
        "current_form_qr_code": agg_res.get("current_form_qr_code"),
        "is_empty": agg_res.get("is_empty"),
        "item": {
            "barcode": item.get("barcode"),
            "type": item.get("type"),
            "name": item.get("name"),
            "price": item.get("price"),
            "unit": item.get("unit"),
            "previous_quantity": item.get("previous_quantity"),
            "addstock": item.get("addstock"),
            "sold": item.get("sold"),
            "current_quantity": item.get("current_quantity"),
            "updated": item.get("updated"),
        },
        "balance_ok": balance_ok
    }

    # Return only "body" as requested
    return {"body": bsonju.dumps(payload)}
