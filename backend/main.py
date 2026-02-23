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

def _extract_handle_or_channel_id(channel_url: str) -> Dict[str, str]:
    """
    Accepts:
      - https://www.youtube.com/@MrBeast
      - https://youtube.com/@MrBeast
      - @MrBeast
      - https://www.youtube.com/channel/UCxxxx
    Returns:
      {"type": "handle", "value": "MrBeast"}  OR  {"type": "channel_id", "value": "UCxxxx"}
    """
    s = channel_url.strip()

    # direct @handle
    if s.startswith("@"):
        return {"type": "handle", "value": s[1:]}

    # /channel/UC...
    m = re.search(r"/channel/(UC[a-zA-Z0-9_-]{10,})", s)
    if m:
        return {"type": "channel_id", "value": m.group(1)}

    # /@handle
    m = re.search(r"/@([a-zA-Z0-9_.-]+)", s)
    if m:
        return {"type": "handle", "value": m.group(1)}

    # last fallback: if user pasted just MrBeast
    if re.fullmatch(r"[a-zA-Z0-9_.-]+", s):
        return {"type": "handle", "value": s}

    raise ValueError("Invalid channel url/handle format")

def _youtube_get_channel_id_by_handle(handle: str, api_key: str) -> str:
    # YouTube Data API supports forHandle in channels.list
    url = "https://www.googleapis.com/youtube/v3/channels"
    params = {
        "part": "id",
        "forHandle": handle,
        "key": api_key,
    }
    r = requests.get(url, params=params, timeout=20)
    r.raise_for_status()
    data = r.json()
    items = data.get("items", [])
    if not items:
        raise ValueError(f"Handle not found: @{handle}")
    return items[0]["id"]

def _youtube_fetch_latest_videos(channel_id: str, api_key: str, limit: int = 5) -> List[Dict[str, Any]]:
    """
    Returns list of video docs (already normalized).
    Uses search.list to fetch latest videos for a channel.
    """
    url = "https://www.googleapis.com/youtube/v3/search"
    params = {
        "part": "snippet",
        "channelId": channel_id,
        "order": "date",
        "maxResults": max(1, min(limit, 50)),
        "type": "video",
        "key": api_key,
    }
    r = requests.get(url, params=params, timeout=25)
    r.raise_for_status()
    data = r.json()

    out = []
    for item in data.get("items", []):
        vid = (item.get("id") or {}).get("videoId")
        sn = item.get("snippet") or {}
        if not vid:
            continue

        thumbs = sn.get("thumbnails") or {}
        # prefer high > medium > default
        thumb_url = None
        for k in ["high", "medium", "default"]:
            if k in thumbs and "url" in thumbs[k]:
                thumb_url = thumbs[k]["url"]
                break

        published_at = sn.get("publishedAt")  # ISO str
        title = sn.get("title")

        out.append(
            {
                "channel_id": channel_id,
                "video_id": vid,
                "title": title,
                "published_at": published_at,
                "thumbnail": thumb_url,
                "video_url": f"https://www.youtube.com/watch?v={vid}",
                "status": "new",
                "created_at": _utcnow(),
            }
        )
    return out

def _ensure_indexes():
    # Unique: each video_id once
    db[VIDEOS_COL].create_index([("video_id", ASCENDING)], unique=True)
    # Channels unique by channel_id
    db[CHANNELS_COL].create_index([("channel_id", ASCENDING)], unique=True)

def _scan_all_channels(limit_per_channel: int = 5) -> Dict[str, Any]:
    """
    Reads channels collection, fetches latest videos, upserts into videos collection.
    """
    if db is None:
        return {"ok": False, "error": "DB not ready"}

    api_key = _get_env("YOUTUBE_API_KEY")
    if not api_key:
        return {"ok": False, "error": "YOUTUBE_API_KEY not set"}

    channels = list(db[CHANNELS_COL].find({}, {"_id": 0}))
    total_new = 0
    per_channel = []

    for ch in channels:
        ch_id = ch.get("channel_id")
        if not ch_id:
            continue

        try:
            videos = _youtube_fetch_latest_videos(ch_id, api_key, limit=limit_per_channel)
            inserted = 0

            for vdoc in videos:
                # Insert if not exists (video_id unique index will enforce)
                try:
                    db[VIDEOS_COL].insert_one(vdoc)
                    inserted += 1
                except Exception:
                    # duplicate -> already exists
                    pass

            total_new += inserted
            per_channel.append({"channel_id": ch_id, "inserted": inserted})
        except Exception as e:
            per_channel.append({"channel_id": ch_id, "error": str(e)})

    return {"ok": True, "channels": len(channels), "total_new": total_new, "details": per_channel}

# -----------------------------
# Startup / Scheduler
# -----------------------------
@app.on_event("startup")
def startup_db():
    global mongo_client, db, scheduler

    mongodb_uri = _get_env("MONGODB_URI")
    if not mongodb_uri:
        print("❌ MONGODB_URI not found")
        return

    mongo_client = MongoClient(
        mongodb_uri,
        server_api=ServerApi("1"),
        serverSelectionTimeoutMS=10000,
    )

    try:
        mongo_client.admin.command("ping")
        db = mongo_client["clipflow_db"]
        _ensure_indexes()
        print("✅ MongoDB Connected Successfully")
    except Exception as e:
        print("❌ Mongo Connection Failed:", e)
        db = None
        return

    # scheduler: every 10 minutes auto scan
    scheduler = BackgroundScheduler(timezone="UTC")
    scheduler.add_job(lambda: _scan_all_channels(limit_per_channel=5), "interval", minutes=10, id="scan_job", replace_existing=True)
    scheduler.start()
    print("✅ Scheduler started: scan every 10 minutes")

@app.on_event("shutdown")
def shutdown_app():
    global scheduler, mongo_client
    try:
        if scheduler:
            scheduler.shutdown(wait=False)
    except Exception:
        pass
    try:
        if mongo_client:
            mongo_client.close()
    except Exception:
        pass

# -----------------------------
# Routes
# -----------------------------
@app.get("/")
def root():
    return {"message": "ClipFlow AI Backend Running 🚀"}

@app.get("/health")
def health():
    return {
        "ok": True,
        "db_ready": db is not None,
        "has_youtube_key": bool(_get_env("YOUTUBE_API_KEY")),
        "time": _utcnow().isoformat(),
    }

@app.get("/mongo-test")
def mongo_test():
    if db is None:
        return {"ok": False, "error": "DB not ready"}
    return {"ok": True, "collections": db.list_collection_names()}

@app.get("/mongo-seed")
def mongo_seed():
    if db is None:
        return {"ok": False, "error": "DB not ready"}

    col = db[VIDEOS_COL]
    doc = {
        "channel_id": "TEST_CHANNEL",
        "video_id": f"TEST_VIDEO_{int(_utcnow().timestamp())}",
        "title": "Hello Mongo",
        "published_at": None,
        "thumbnail": None,
        "video_url": None,
        "status": "new",
        "created_at": _utcnow(),
    }
    result = col.insert_one(doc)
    return {"ok": True, "inserted_id": str(result.inserted_id)}

@app.get("/channels")
def list_channels():
    if db is None:
        return {"ok": False, "error": "DB not ready"}
    items = list(db[CHANNELS_COL].find({}, {"_id": 0}))
    return {"ok": True, "count": len(items), "items": items}

@app.get("/add-channel")
def add_channel(channel_url: str):
    """
    Example:
      /add-channel?channel_url=https://www.youtube.com/@MrBeast
    Stores:
      channels: {channel_id, url, created_at}
    """
    if db is None:
        return {"ok": False, "error": "DB not ready"}

    api_key = _get_env("YOUTUBE_API_KEY")
    if not api_key:
        return {"ok": False, "error": "YOUTUBE_API_KEY not set"}

    try:
        parsed = _extract_handle_or_channel_id(channel_url)
        if parsed["type"] == "channel_id":
            channel_id = parsed["value"]
        else:
            channel_id = _youtube_get_channel_id_by_handle(parsed["value"], api_key)

        doc = {
            "channel_id": channel_id,
            "url": channel_url.strip(),
            "created_at": _utcnow(),
        }

        # upsert-like behavior: insert if new, else update url
        db[CHANNELS_COL].update_one(
            {"channel_id": channel_id},
            {"$set": {"url": doc["url"]}, "$setOnInsert": {"created_at": doc["created_at"]}},
            upsert=True,
        )

        return {"ok": True, "channel_id": channel_id, "saved_url": doc["url"]}

    except Exception as e:
        return {"ok": False, "error": str(e)}

@app.get("/scan-channels")
def scan_channels(limit_per_channel: int = 5):
    """
    Fetch latest videos for all channels and save to DB.
    """
    limit_per_channel = max(1, min(limit_per_channel, 50))
    return _scan_all_channels(limit_per_channel=limit_per_channel)

@app.get("/videos")
def list_videos(channel_id: Optional[str] = None, limit: int = 50):
    """
    List saved videos.
    /videos
    /videos?channel_id=UCxxxx
    """
    if db is None:
        return {"ok": False, "error": "DB not ready"}

    limit = max(1, min(limit, 200))
    q = {}
    if channel_id:
        q["channel_id"] = channel_id

    items = list(
        db[VIDEOS_COL]
        .find(q, {"_id": 0})
        .sort("created_at", -1)
        .limit(limit)
    )
    return {"ok": True, "count": len(items), "items": items}
