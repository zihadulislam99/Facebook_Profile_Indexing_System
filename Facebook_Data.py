# ============================================================
# FACEBOOK OSINT PROFILE INGESTION PIPELINE
# SerpAPI → Normalize → MongoDB (Safe Upsert, NO META CONFLICT)
# ============================================================

from serpapi import GoogleSearch
import json
import hashlib
from pymongo import MongoClient
from datetime import datetime

# ===================== CONFIG =====================

SERPAPI_KEY = "Your_Serpapi_API_KEY"
MONGODB_URL = "mongodb://localhost:27017/"
DB_NAME = "knowledge-base"
COLLECTION_NAME = "social_osint_profiles"

# ===================== DB CONNECT =====================

client = MongoClient(MONGODB_URL)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

collection.create_index(
    "identity.profile_id",
    unique=True,
    partialFilterExpression={"identity.profile_id": {"$exists": True}}
)

# ===================== UTILITIES =====================

def local_actor_id(username, facebook_id):
    base = f"{username}_{facebook_id}"
    return "FB_ACTOR_" + hashlib.md5(base.encode()).hexdigest()


def normalize_followers(value):
    if not value:
        return None
    value = value.upper().replace(",", "").strip()
    try:
        if value.endswith("K"):
            return int(float(value[:-1]) * 1_000)
        if value.endswith("M"):
            return int(float(value[:-1]) * 1_000_000)
        if value.endswith("B"):
            return int(float(value[:-1]) * 1_000_000_000)
        return int(value)
    except:
        return None

# ===================== TRANSFORM =====================

def transform_profile(raw):
    fb_id = raw.get("id")
    url = raw.get("url", "")
    username = raw.get("username") or url.split(".com/")[-1]
    now = datetime.utcnow()
    return {
        "profile": {
            "actor": {
                "local_actor_id": local_actor_id(username, fb_id),
                "platform": "facebook",
                "profile_type": raw.get("profile_type", "PROFILE"),
                "verified": raw.get("verified", False)
            },
            "identity": {
                "profile_id": fb_id,
                "username": username,
                "name": raw.get("name", ""),
                "gender": raw.get("gender", "unknown"),
                "category": raw.get("category", "")
            },
            "profile": {
                "url": url,
                "intro_text": raw.get("profile_intro_text", ""),
                "current_city": raw.get("current_city", ""),
                "hometown": raw.get("hometown", ""),
                "relationship": raw.get("relationship", "")
            },
            "media": {
                "profile_picture": raw.get("profile_picture", ""),
                "cover_photo": raw.get("cover_photo", "")
            },
            "metrics": {
                "followers": {
                    "raw": raw.get("followers", ""),
                    "normalized": normalize_followers(raw.get("followers"))
                },
                "following": {
                    "raw": raw.get("following", ""),
                    "normalized": normalize_followers(raw.get("following"))
                }
            },
            "education": [
                {
                    "institution": e.get("name", ""),
                    "description": e.get("title", ""),
                    "facebook_link": e.get("facebook_link", "")
                } for e in raw.get("educations", [])
            ],
            "work": [
                {
                    "title": w.get("title", ""),
                    "organization": w.get("name", ""),
                    "facebook_link": w.get("facebook_link", "")
                } for w in raw.get("works", [])
            ],
            "photos": [
                {
                    "url": p.get("link", ""),
                    "owner_id": p.get("owner", {}).get("id", ""),
                    "owner_type": p.get("owner", {}).get("type", "")
                } for p in raw.get("photos", [])
            ],
            "metadata": {
                "created_at": now,
                "last_update": now,
                "source": "SerpAPI.engine.facebook_profile"
            }
        }
    }

# ===================== DB UPSERT (SAFE, NO CONFLICT) =====================

def upsert_profile(schema: dict):
    identity = schema.get("profile", {}).get("identity", {})
    profile_id = identity.get("profile_id")
    if not profile_id:
        return {"status": "skipped", "reason": "missing_profile_id"}
    now = datetime.utcnow()
    result = collection.update_one(
        {"profile.identity.profile_id": profile_id}, 
        {
            "$setOnInsert": {
                **schema,
            }
        }, 
        upsert=True
    )

    if result.upserted_id:
        return {"status": "inserted", "profile_id": profile_id}
    else:
        return {"status": "exists", "profile_id": profile_id}


# ===================== MAIN FETCH =====================

def fetch_facebook_profile(username: str):
    params = {
        "engine": "facebook_profile",
        "profile_id": username,
        "api_key": SERPAPI_KEY
    }

    try:
        search = GoogleSearch(params)
        raw_data = search.get_dict().get("profile_results")
        if not raw_data:
            return {"status": "not_found", "username": username}
        schema = transform_profile(raw_data)
        db_result = upsert_profile(schema)
        return {"db": db_result, "profile": schema}
    
    except Exception as e:
        return {"status": "error", "username": username, "error": str(e)}

# ===================== RUN =====================

if __name__ == "__main__":
    result = fetch_facebook_profile("zuck")
    print(json.dumps(result, indent=2, ensure_ascii=False))








# print(upsert_profile(a))
