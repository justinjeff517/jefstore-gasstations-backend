import os, json
from pymongo import MongoClient
from datetime import datetime, timedelta, timezone

client = MongoClient(os.getenv("MONGODB_URI"))
db = client[os.getenv("MONGODB_DATABASE")]
coll = db["pump_inventories"]

def resp(code, obj):
    return {
        "statusCode": code,
        "headers": {"content-type": "application/json"},
        "body": json.dumps(obj, default=str),
    }

def _parse_iso(dtval):
    if isinstance(dtval, datetime):
        return dtval
    if isinstance(dtval, str):
        s = dtval.rstrip("Z") + ("+00:00" if dtval.endswith("Z") else "")
        try:
            return datetime.fromisoformat(s)
        except Exception:
            return None
    return None

def _to_utc(dt):
    if dt is None:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)

def main(args):
    try:
        location = args.get("location")
        pump_id = args.get("pump_id")
        if not location or not pump_id:
            return resp(400, {"ok": False, "error": "location and pump_id are required"})

        doc = coll.find_one(
            {"location": location, "pump_id": str(pump_id)},
            {"_id": 0},
            sort=[("date", -1), ("created_at", -1)],
        )
        if not doc:
            return resp(404, {"ok": False, "error": "not found"})

        dt = _parse_iso(doc.get("date"))
        dt_utc = _to_utc(dt)

        if dt_utc:
            next_dt = dt_utc + timedelta(days=1)
            iso = next_dt.isoformat().replace("+00:00", "Z")
            doc["next_date"] = iso

            manila_offset = timedelta(hours=8)
            doc_day_manila = (dt_utc + manila_offset).date()
            today_manila = (datetime.now(timezone.utc) + manila_offset).date()
            doc["is_matching_today"] = (doc_day_manila == today_manila)
        else:
            doc["is_matching_today"] = False

        return resp(200, doc)
    except Exception as e:
        return resp(500, {"ok": False, "error": str(e)})
