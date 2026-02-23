import os
import re
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

import requests
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient, ASCENDING
from pymongo.server_api import ServerApi
from apscheduler.schedulers.background import BackgroundScheduler

# -----------------------------
# App
# -----------------------------
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

mongo_client: Optional[MongoClient] = None
db = None
scheduler: Optional[BackgroundScheduler] = None

CHANNELS_COL = "channels"
VIDEOS_COL = "videos"

# -----------------------------
# Helpers
# -----------------------------
def _utcnow():
    return datetime.now(timezone.utc)

def _get_env(name: str) -> Optional[str]:
    v = os.getenv(name)
    return v.strip() if v else None

# -----------------------------
# YouTube Helpers
# -----------------------------
def _extract_handle_or_channel_id(channel_url: str) -> Dict[str, str]:
    s = channel_url.strip()

    if s.startswith("@"):
        return {"type": "handle", "value": s[1:]}

    m = re.search(r"/channel/(UC[a-zA-Z0-9_-]{10,})", s)
    if m:
        return {"type": "channel_id", "value": m.group(1)}

    m = re.search(r"/@([a-zA-Z0-9_.-]+)", s)
    if m:
        return {"type": "handle", "value": m.group(1)}

    if re.fullmatch(r"[a-zA-Z0-9_.-]+", s):
        return {"type": "handle", "value": s}

    raise ValueError("Invalid channel format")

def _youtube_get_channel_id_by_handle(handle: str, api_key: str) -> str:
    url = "https://www.googleapis.com/youtube/v3/channels"
    params = {
        "part": "id",
        "forHandle": handle,
        "key": api_key,
    }
    r = requests.get(url, params=params, timeout=20)
    r.raise_for_status()
    items = r.json().get("items", [])
    if not items:
        raise ValueError(f"Handle not found: @{handle}")
    return items[0]["id"]

def _youtube_fetch_latest_videos(channel_id: str, api_key: str, limit: int = 5):
    url = "https://www.googleapis.com/youtube/v3/search"
    params = {
        "part": "snippet",
        "channelId": channel_id,
        "order": "date",
        "maxResults": limit,
        "type": "video",
        "key": api_key,
    }
    r = requests.get(url, params=params, timeout=25)
    r.raise_for_status()

    out = []
    for item in r.json().get("items", []):
        vid = item["id"].get("videoId")
        sn = item.get("snippet", {})

        if not vid:
            continue

        thumbs = sn.get("thumbnails", {})
        thumb_url = None
        for k in ["high", "medium", "default"]:
            if k in thumbs:
                thumb_url = thumbs[k]["url"]
                break

        out.append(
            {
                "channel_id": channel_id,
                "video_id": vid,
                "title": sn.get("title"),
                "published_at": sn.get("publishedAt"),
                "thumbnail": thumb_url,
                "video_url": f"https://www.youtube.com/watch?v={vid}",
                "status": "new",
                "created_at": _utcnow(),
            }
        )
    return out

# -----------------------------
# DB Setup
# -----------------------------
def _ensure_indexes():
    try:
        db[VIDEOS_COL].create_index([("video_id", ASCENDING)], unique=True)
        db[CHANNELS_COL].create_index([("channel_id", ASCENDING)], unique=True)
        print("✅ Indexes ready")
    except Exception as e:
        print("Index warning:", e)

# -----------------------------
# Scanner
# -----------------------------
def _scan_all_channels(limit_per_channel: int = 5):
    if db is None:
        return {"ok": False, "error": "DB not ready"}

    api_key = _get_env("YOUTUBE_API_KEY")
    if not api_key:
        return {"ok": False, "error": "YOUTUBE_API_KEY not set"}

    channels = list(db[CHANNELS_COL].find({}, {"_id": 0}))
    total_new = 0

    for ch in channels:
        try:
            videos = _youtube_fetch_latest_videos(ch["channel_id"], api_key, limit_per_channel)

            for v in videos:
                try:
                    db[VIDEOS_COL].insert_one(v)
                    total_new += 1
                except:
                    pass  # duplicate video_id

        except Exception as e:
            print("Scan error:", e)

    return {"ok": True, "total_new": total_new}

# -----------------------------
# Startup
# -----------------------------
@app.on_event("startup")
def startup():
    global mongo_client, db, scheduler

    mongodb_uri = _get_env("MONGODB_URI")
    if not mongodb_uri:
        print("❌ MONGODB_URI not set")
        return

    try:
        mongo_client = MongoClient(
            mongodb_uri,
            server_api=ServerApi("1"),
            serverSelectionTimeoutMS=10000,
        )
        mongo_client.admin.command("ping")
        db = mongo_client["clipflow_db"]
        print("✅ Mongo Connected")

        _ensure_indexes()

    except Exception as e:
        print("Mongo connection failed:", e)
        return  # db None yapmıyoruz

    # Scheduler
    scheduler = BackgroundScheduler(timezone="UTC")
    scheduler.add_job(
        lambda: _scan_all_channels(5),
        "interval",
        minutes=10,
        id="scan_job",
        replace_existing=True,
    )
    scheduler.start()
    print("✅ Scheduler running (10 min interval)")

@app.on_event("shutdown")
def shutdown():
    if scheduler:
        scheduler.shutdown(wait=False)
    if mongo_client:
        mongo_client.close()

# -----------------------------
# Routes
# -----------------------------
@app.get("/")
def root():
    return {"message": "ClipFlow Backend Running 🚀"}

@app.get("/health")
def health():
    return {
        "ok": True,
        "db_ready": db is not None,
        "has_youtube_key": bool(_get_env("YOUTUBE_API_KEY")),
        "time": _utcnow().isoformat(),
    }

@app.get("/add-channel")
def add_channel(channel_url: str):
    if db is None:
        return {"ok": False, "error": "DB not ready"}

    api_key = _get_env("YOUTUBE_API_KEY")
    if not api_key:
        return {"ok": False, "error": "YOUTUBE_API_KEY not set"}

    parsed = _extract_handle_or_channel_id(channel_url)

    if parsed["type"] == "channel_id":
        channel_id = parsed["value"]
    else:
        channel_id = _youtube_get_channel_id_by_handle(parsed["value"], api_key)

    db[CHANNELS_COL].update_one(
        {"channel_id": channel_id},
        {"$set": {"url": channel_url, "updated_at": _utcnow()},
         "$setOnInsert": {"created_at": _utcnow()}},
        upsert=True,
    )

    return {"ok": True, "channel_id": channel_id}

@app.get("/scan-channels")
def scan():
    return _scan_all_channels(5)

@app.get("/videos")
def list_videos(limit: int = 50):
    if db is None:
        return {"ok": False, "error": "DB not ready"}

    items = list(
        db[VIDEOS_COL]
        .find({}, {"_id": 0})
        .sort("created_at", -1)
        .limit(limit)
    )
    return {"ok": True, "count": len(items), "items": items}
