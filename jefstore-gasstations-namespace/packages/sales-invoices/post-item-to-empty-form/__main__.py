import os, json, uuid
from datetime import datetime, timezone

from pymongo import MongoClient, ReturnDocument
from bson import json_util as bsonju

CORS_HEADERS = {
  "Content-Type": "application/json",
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, Authorization"
}

def _resp(payload, status=200):
    return {"statusCode": status, "headers": CORS_HEADERS, "body": bsonju.dumps(payload)}

def _num(x):
    try:
        return float(x)
    except Exception:
        return None

def _err(reason, message, status=400, extra=None):
    payload = {"ok": False, "reason": reason, "message": message}
    if extra: payload.update(extra)
    return _resp(payload, status=status)

def main(args):
    # Preflight
    if args.get("__ow_method", "").upper() == "OPTIONS":
        return {"statusCode": 204, "headers": CORS_HEADERS, "body": ""}

    uri = os.getenv("MONGODB_URI")
    db_name = os.getenv("MONGODB_DATABASE")
    if not uri or not db_name:
        return _err("config_error", "Missing MONGODB_URI or MONGODB_DATABASE", status=500)

    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        col = client[db_name]["sales_invoices"]
    except Exception as e:
        return _err("connect_error", f"DB connect failed: {e}", status=500)

    payload = {
        "receipt_number": args.get("receipt_number"),
        "customer_name": args.get("customer_name"),
        "type": args.get("type"),
        "vatable_sales": args.get("vatable_sales"),
        "vat_amount": args.get("vat_amount"),
        "total_amount": args.get("total_amount"),
    }

    required = ["receipt_number", "customer_name", "type", "vatable_sales", "vat_amount", "total_amount"]
    missing = [k for k in required if payload.get(k) in (None, "")]
    if missing:
        return _err("validation_error", "Missing required fields", extra={"missing": missing})

    if payload["type"] not in {"fuel", "lubricant"}:
        return _err("validation_error", "type must be 'fuel' or 'lubricant'", extra={"got": payload["type"]})

    vs = _num(payload["vatable_sales"])
    va = _num(payload["vat_amount"])
    ta = _num(payload["total_amount"])
    if None in (vs, va, ta):
        return _err("validation_error", "vatable_sales, vat_amount, total_amount must be numbers")

    if abs((vs + va) - ta) > 0.01:
        return _err(
            "vat_mismatch",
            "vatable_sales + vat_amount must equal total_amount (±0.01)",
            extra={"vatable_sales": vs, "vat_amount": va, "total_amount": ta}
        )

    try:
        open_forms = list(col.find(
            {"is_empty": True},
            {"_id": 1, "id": 1, "items.item_number": 1, "items.receipt_number": 1}
        ).limit(2))
    except Exception as e:
        return _err("query_error", f"Failed to query open form: {e}", status=500)

    if len(open_forms) != 1:
        return _err(
            "need_exactly_one_open_form",
            "Expected exactly one document with is_empty=True",
            extra={"count": len(open_forms)}
        )

    doc = open_forms[0]
    existing_items = doc.get("items") or []
    if any((it.get("receipt_number") == payload["receipt_number"]) for it in existing_items):
        return _err(
            "duplicate_receipt_number",
            "receipt_number already exists in the open form",
            extra={"receipt_number": payload["receipt_number"], "document_id": doc.get("id")}
        )

    def _parse_num(s):
        try:
            return int(str(s).strip())
        except Exception:
            return None

    now = datetime.now(timezone.utc)
    new_item_base = {
        "id": str(uuid.uuid4()),
        "receipt_number": payload["receipt_number"],
        "customer_name": payload["customer_name"],
        "type": payload["type"],
        "vatable_sales": float(f"{vs:.2f}"),
        "vat_amount": float(f"{va:.2f}"),
        "total_amount": float(f"{ta:.2f}"),
        "created": now,
        "updated": now,
    }

    MAX_RETRIES = 3
    attempt = 0
    updated_doc = None
    last_next_item_number = None

    while attempt < MAX_RETRIES and updated_doc is None:
        attempt += 1

        fresh = col.find_one({"_id": doc["_id"], "is_empty": True}, {"_id": 1, "id": 1, "items.item_number": 1})
        if not fresh:
            return _err("open_form_changed", "Open form is no longer available or not is_empty=True")

        fresh_items = fresh.get("items") or []
        nums = []
        for it in fresh_items:
            n = _parse_num(it.get("item_number"))
            if n is not None:
                nums.append(n)

        next_item_number_int = (max(nums) + 1) if nums else 1
        next_item_number = str(next_item_number_int)
        last_next_item_number = next_item_number

        filter_q = {
            "_id": doc["_id"],
            "is_empty": True,
            "items.item_number": {"$ne": next_item_number},
        }

        new_item = {**new_item_base, "item_number": next_item_number}

        try:
            updated_doc = col.find_one_and_update(
                filter_q,
                {"$push": {"items": new_item}},
                return_document=ReturnDocument.AFTER,
            )
        except Exception as e:
            return _err("update_error", f"Failed to push item: {e}", status=500)

    if updated_doc is None:
        return _err(
            "conflict",
            "Failed to insert after retries (likely concurrent inserts). Try again.",
            extra={"last_candidate_item_number": last_next_item_number}
        )

    added = next((it for it in (updated_doc.get("items") or []) if it.get("id") == new_item_base["id"]), None)
    if not added:
        return _err(
            "insert_unknown_state",
            "Insert may have raced; added item not found on document.",
            extra={"document_id": updated_doc.get("id")}
        )

    return _resp({
        "ok": True,
        "message": "item added to open form (is_empty unchanged)",
        "document_id": updated_doc.get("id"),
        "item": added
    }, status=200)
