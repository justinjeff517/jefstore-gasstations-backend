# Serverless: delete item(s) by receipt_number from the ONLY empty form and renumber remaining items.
# Responds via {"body": "..."} for platforms like DigitalOcean Functions / OpenWhisk.

import os, json
from pymongo import MongoClient
from bson import json_util as bsonju

def _err(reason, message, status=400, extra=None):
    payload = {"ok": False, "reason": reason, "message": message}
    if extra: payload.update(extra)
    return {"body": json.dumps(payload), "statusCode": status}

def _ok(data, status=200):
    return {"body": bsonju.dumps(data), "statusCode": status}

def _created_iso(it):
    v = it.get("created")
    if isinstance(v, dict) and "$date" in v: return v["$date"]
    if isinstance(v, str): return v
    return ""

def _num_item_no(s):
    try: return int(str(s))
    except: return 10**9

def main(args):
    # --- Config ---
    uri = os.getenv("MONGODB_URI")
    db_name = os.getenv("MONGODB_DATABASE")
    if not uri or not db_name:
        return _err("config_error", "Missing MONGODB_URI or MONGODB_DATABASE")

    receipt_number = (args or {}).get("receipt_number")
    if not receipt_number or not str(receipt_number).strip():
        return _err("bad_request", "Missing 'receipt_number' in args")

    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        col = client[db_name]["sales_invoices"]
    except Exception as e:
        return _err("connect_error", f"DB connect failed: {e}")

    # --- Find exactly one empty form ---
    try:
        docs = list(col.find({"is_empty": True}).limit(2))
    except Exception as e:
        return _err("query_error", f"Find empty form failed: {e}")

    if len(docs) != 1:
        return _err("need_exactly_one_empty_form", "Expected exactly one is_empty=True document", extra={"count": len(docs)})

    doc = docs[0]
    before_items = list(doc.get("items", []))
    before_len = len(before_items)

    # --- Pull matching items ---
    try:
        pull_res = col.update_one({"_id": doc["_id"]}, {"$pull": {"items": {"receipt_number": receipt_number}}})
    except Exception as e:
        return _err("update_error", f"$pull failed: {e}")

    # --- Re-read and renumber deterministically ---
    updated = col.find_one({"_id": doc["_id"]})
    items_after_delete = list(updated.get("items", []))
    removed_count = max(before_len - len(items_after_delete), 0)

    set_modified = 0
    final_numbers = []
    if items_after_delete:
        items_sorted = sorted(
            items_after_delete,
            key=lambda it: (_created_iso(it) == "", _created_iso(it), _num_item_no(it.get("item_number", "0")))
        )
        for i, it in enumerate(items_sorted, start=1):
            it["item_number"] = str(i)
        try:
            set_res = col.update_one({"_id": doc["_id"]}, {"$set": {"items": items_sorted}})
            set_modified = set_res.modified_count
        except Exception as e:
            return _err("update_error", f"$set renumber failed: {e}")

        final_doc = col.find_one({"_id": doc["_id"]}, {"items": 1, "id": 1, "current_form_qr_code": 1, "is_empty": 1})
        final_numbers = [it.get("item_number") for it in final_doc.get("items", [])]
    else:
        final_doc = {
            "id": updated.get("id"),
            "current_form_qr_code": updated.get("current_form_qr_code"),
            "is_empty": updated.get("is_empty"),
            "items": []
        }

    # --- Response ---
    return _ok({
        "ok": True,
        "message": "Deleted by receipt_number, then renumbered remaining items (is_empty unchanged).",
        "receipt_number": receipt_number,
        "removed_count": removed_count,
        "matched_doc": pull_res.matched_count,
        "modified_doc_pull": pull_res.modified_count,
        "modified_doc_set": set_modified,
        "document_meta": {
            "id": final_doc.get("id"),
            "current_form_qr_code": final_doc.get("current_form_qr_code"),
            "is_empty": final_doc.get("is_empty"),
            "items_length": len(final_numbers)
        },
        "item_numbers_after": final_numbers
    })
