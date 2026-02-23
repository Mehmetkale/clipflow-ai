import os
import re
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

import requests
from fastapi import FastAPI, Body
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

# -----------------------------
# Collections
# -----------------------------
CHANNELS_COL = "channels"
VIDEOS_COL = "videos"

TRANSCRIPTS_COL = "transcripts"
SHORTS_CANDIDATES_COL = "shorts_candidates"
SHORTS_EXPORTS_COL = "shorts_exports"  # ileride export/editleme için

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
    """
    Accepts:
      - https://www.youtube.com/@MrBeast
      - https://youtube.com/@MrBeast
      - @MrBeast
      - https://www.youtube.com/channel/UCxxxx
      - UCxxxx (direct)
    """
    s = channel_url.strip()

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

    # direct UCxxxx
    if s.startswith("UC") and re.fullmatch(r"UC[a-zA-Z0-9_-]{10,}", s):
        return {"type": "channel_id", "value": s}

    # fallback: just "MrBeast"
    if re.fullmatch(r"[a-zA-Z0-9_.-]+", s):
        return {"type": "handle", "value": s}

    raise ValueError("Invalid channel format")

def _youtube_get_channel_id_by_handle(handle: str, api_key: str) -> str:
    """
    YouTube Data API supports 'forHandle' in channels.list
    """
    url = "https://www.googleapis.com/youtube/v3/channels"
    params = {"part": "id", "forHandle": handle, "key": api_key}
    r = requests.get(url, params=params, timeout=20)
    r.raise_for_status()
    items = r.json().get("items", [])
    if not items:
        raise ValueError(f"Handle not found: @{handle}")
    return items[0]["id"]

def _youtube_fetch_latest_videos(channel_id: str, api_key: str, limit: int = 5) -> List[Dict[str, Any]]:
    """
    Fetch latest videos via search.list ordered by date.
    """
    limit = max(1, min(limit, 50))
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

    out: List[Dict[str, Any]] = []
    for item in r.json().get("items", []):
        vid = (item.get("id") or {}).get("videoId")
        sn = item.get("snippet") or {}
        if not vid:
            continue

        thumbs = sn.get("thumbnails") or {}
        thumb_url = None
        for k in ["high", "medium", "default"]:
            if k in thumbs and "url" in thumbs[k]:
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
# Shorts Helpers
# -----------------------------
def _generate_shorts_candidates_from_text(text: str) -> List[Dict[str, Any]]:
    """
    Şimdilik basit kural tabanlı:
    - Noktalara göre böl
    - Uzun cümleleri "hook" olarak al
    - İlk 10 taneyi döndür
    İleride OpenAI ile çok daha iyi hale getireceğiz.
    """
    # basit split
    raw = [s.strip() for s in text.replace("\n", " ").split(".") if s.strip()]
    sentences = [s for s in raw if len(s) >= 40]

    candidates: List[Dict[str, Any]] = []
    for i, s in enumerate(sentences[:10]):
        score = min(100, 60 + (len(s) // 5))
        candidates.append(
            {
                "index": i,
                "hook": s[:160],
                "reason": "Strong statement / emotional hook (heuristic)",
                "score": int(score),
            }
        )
    return candidates

# -----------------------------
# DB Setup
# -----------------------------
def _ensure_indexes():
    """
    Unique indexler:
      - videos.video_id unique
      - channels.channel_id unique
      - transcripts.video_id unique
      - shorts_candidates.video_id unique
    """
    try:
        db[VIDEOS_COL].create_index([("video_id", ASCENDING)], unique=True)
        db[CHANNELS_COL].create_index([("channel_id", ASCENDING)], unique=True)
        db[TRANSCRIPTS_COL].create_index([("video_id", ASCENDING)], unique=True)
        db[SHORTS_CANDIDATES_COL].create_index([("video_id", ASCENDING)], unique=True)
        print("✅ Indexes ready")
    except Exception as e:
        print("Index warning:", e)

# -----------------------------
# Scanner
# -----------------------------
def _scan_all_channels(limit_per_channel: int = 5) -> Dict[str, Any]:
    if db is None:
        return {"ok": False, "error": "DB not ready"}

    api_key = _get_env("YOUTUBE_API_KEY")
    if not api_key:
        return {"ok": False, "error": "YOUTUBE_API_KEY not set"}

    channels = list(db[CHANNELS_COL].find({}, {"_id": 0}))
    total_new = 0
    details = []

    for ch in channels:
        ch_id = ch.get("channel_id")
        if not ch_id:
            continue

        inserted = 0
        try:
            videos = _youtube_fetch_latest_videos(ch_id, api_key, limit_per_channel)
            for v in videos:
                try:
                    db[VIDEOS_COL].insert_one(v)
                    inserted += 1
                    total_new += 1
                except Exception:
                    # duplicate video_id
                    pass
            details.append({"channel_id": ch_id, "inserted": inserted})
        except Exception as e:
            details.append({"channel_id": ch_id, "error": str(e)})

    return {"ok": True, "channels": len(channels), "total_new": total_new, "details": details}

# -----------------------------
# Startup / Shutdown
# -----------------------------
@app.on_event("startup")
def startup():
    global mongo_client, db, scheduler

    mongodb_uri = _get_env("MONGODB_URI")
    if not mongodb_uri:
        print("❌ MONGODB_URI not set")
        db = None
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
        db = None
        return

    # Scheduler: every 10 minutes
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

    try:
        parsed = _extract_handle_or_channel_id(channel_url)
        if parsed["type"] == "channel_id":
            channel_id = parsed["value"]
        else:
            channel_id = _youtube_get_channel_id_by_handle(parsed["value"], api_key)

        now = _utcnow()
        db[CHANNELS_COL].update_one(
            {"channel_id": channel_id},
            {
                "$set": {"url": channel_url.strip(), "updated_at": now},
                "$setOnInsert": {"created_at": now},
            },
            upsert=True,
        )

        return {"ok": True, "channel_id": channel_id, "saved_url": channel_url.strip()}

    except Exception as e:
        return {"ok": False, "error": str(e)}

@app.get("/scan-channels")
def scan_channels(limit_per_channel: int = 5):
    return _scan_all_channels(limit_per_channel)

@app.get("/videos")
def list_videos(limit: int = 50, channel_id: Optional[str] = None):
    if db is None:
        return {"ok": False, "error": "DB not ready"}

    limit = max(1, min(limit, 200))
    q: Dict[str, Any] = {}
    if channel_id:
        q["channel_id"] = channel_id

    items = list(
        db[VIDEOS_COL]
        .find(q, {"_id": 0})
        .sort("created_at", -1)
        .limit(limit)
    )
    return {"ok": True, "count": len(items), "items": items}

@app.get("/channels")
def list_channels():
    if db is None:
        return {"ok": False, "error": "DB not ready"}
    items = list(db[CHANNELS_COL].find({}, {"_id": 0}).sort("created_at", -1))
    return {"ok": True, "count": len(items), "items": items}

# -----------------------------
# Transcript + Shorts Routes
# -----------------------------
@app.post("/transcript/upload")
def upload_transcript(video_id: str, text: str = Body(...)):
    if db is None:
        return {"ok": False, "error": "DB not ready"}

    doc = {
        "video_id": video_id,
        "text": text,
        "created_at": _utcnow(),
    }

    db[TRANSCRIPTS_COL].update_one(
        {"video_id": video_id},
        {"$set": doc},
        upsert=True
    )

    return {"ok": True, "video_id": video_id}

@app.post("/shorts/candidates")
def generate_candidates(video_id: str):
    if db is None:
        return {"ok": False, "error": "DB not ready"}

    tr = db[TRANSCRIPTS_COL].find_one({"video_id": video_id})
    if not tr:
        return {"ok": False, "error": "Transcript not found"}

    candidates = _generate_shorts_candidates_from_text(tr.get("text") or "")

    db[SHORTS_CANDIDATES_COL].update_one(
        {"video_id": video_id},
        {"$set": {
            "video_id": video_id,
            "candidates": candidates,
            "created_at": _utcnow(),
        }},
        upsert=True
    )

    return {"ok": True, "count": len(candidates), "candidates": candidates}

@app.get("/shorts/list")
def list_shorts(video_id: str):
    if db is None:
        return {"ok": False, "error": "DB not ready"}

    data = db[SHORTS_CANDIDATES_COL].find_one({"video_id": video_id}, {"_id": 0})
    if not data:
        return {"ok": False, "error": "No shorts found"}

    return {"ok": True, "data": data}
