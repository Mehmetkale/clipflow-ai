import os
import re
import math
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

import requests
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from pymongo import MongoClient, ASCENDING
from pymongo.server_api import ServerApi
from pymongo.errors import DuplicateKeyError
from apscheduler.schedulers.background import BackgroundScheduler

from youtube_transcript_api import YouTubeTranscriptApi


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
SHORTS_PLANS_COL = "shorts_plans"
TRANSCRIPTS_COL = "transcripts"


# -----------------------------
# Helpers
# -----------------------------
def _utcnow():
    return datetime.now(timezone.utc)


def _get_env(name: str) -> Optional[str]:
    v = os.getenv(name)
    return v.strip() if v else None


def _normalize_text(s: str) -> str:
    s = (s or "").replace("\r", " ").replace("\n", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


# -----------------------------
# Models (Transcript Ingest)
# -----------------------------
class TranscriptIngestRequest(BaseModel):
    video_id: str
    transcript_text: str
    language: Optional[str] = "en"
    source: Optional[str] = "manual"  # manual | browser | n8n | other
    also_plan: Optional[bool] = True
    max_segments: Optional[int] = 6


class PlanFromTextRequest(BaseModel):
    video_id: str
    transcript_text: str
    max_segments: Optional[int] = 6
    wps: Optional[float] = 2.6  # words/sec for pseudo timestamps
    language: Optional[str] = "en"
    source: Optional[str] = "manual"


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
      - UCxxxx
    Returns:
      {"type": "handle", "value": "MrBeast"}  OR  {"type": "channel_id", "value": "UCxxxx"}
    """
    s = channel_url.strip()

    if s.startswith("@"):
        return {"type": "handle", "value": s[1:]}

    m = re.search(r"/channel/(UC[a-zA-Z0-9_-]{10,})", s)
    if m:
        return {"type": "channel_id", "value": m.group(1)}

    m = re.search(r"/@([a-zA-Z0-9_.-]+)", s)
    if m:
        return {"type": "handle", "value": m.group(1)}

    if re.fullmatch(r"UC[a-zA-Z0-9_-]{10,}", s):
        return {"type": "channel_id", "value": s}

    if re.fullmatch(r"[a-zA-Z0-9_.-]+", s):
        return {"type": "handle", "value": s}

    raise ValueError("Invalid channel format")


def _youtube_get_channel_id_by_handle(handle: str, api_key: str) -> str:
    url = "https://www.googleapis.com/youtube/v3/channels"
    params = {"part": "id", "forHandle": handle, "key": api_key}
    r = requests.get(url, params=params, timeout=20)
    r.raise_for_status()

    items = (r.json() or {}).get("items", [])
    if not items:
        raise ValueError(f"Handle not found: @{handle}")
    return items[0]["id"]


def _youtube_fetch_latest_videos(channel_id: str, api_key: str, limit: int = 5) -> List[Dict[str, Any]]:
    """
    Uses search.list to fetch latest videos for a channel.
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

    out = []
    data = r.json() or {}
    for item in data.get("items", []):
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
                "updated_at": _utcnow(),
            }
        )

    return out


# -----------------------------
# DB Setup
# -----------------------------
def _ensure_indexes():
    try:
        db[VIDEOS_COL].create_index([("video_id", ASCENDING)], unique=True)
    except Exception as e:
        print("Index warning videos.video_id:", e)

    try:
        db[CHANNELS_COL].create_index([("channel_id", ASCENDING)], unique=True)
    except Exception as e:
        print("Index warning channels.channel_id:", e)

    try:
        db[SHORTS_PLANS_COL].create_index([("video_id", ASCENDING)], unique=True)
    except Exception as e:
        print("Index warning shorts_plans.video_id:", e)

    try:
        db[TRANSCRIPTS_COL].create_index([("video_id", ASCENDING)], unique=True)
    except Exception as e:
        print("Index warning transcripts.video_id:", e)

    try:
        db[VIDEOS_COL].create_index([("status", ASCENDING), ("created_at", ASCENDING)])
    except Exception as e:
        print("Index warning videos.status:", e)

    print("✅ Indexes ready")


# -----------------------------
# Scanner
# -----------------------------
def _scan_all_channels(limit_per_channel: int = 5) -> Dict[str, Any]:
    if db is None:
        return {"ok": False, "error": "DB not ready"}

    api_key = _get_env("YOUTUBE_API_KEY")
    if not api_key:
        return {"ok": False, "error": "YOUTUBE_API_KEY not set"}

    limit_per_channel = max(1, min(limit_per_channel, 50))

    channels = list(db[CHANNELS_COL].find({"active": True}, {"_id": 0}))
    total_new = 0
    details = []

    for ch in channels:
        ch_id = ch.get("channel_id")
        if not ch_id:
            continue

        inserted = 0
        try:
            videos = _youtube_fetch_latest_videos(ch_id, api_key, limit=limit_per_channel)
            for v in videos:
                try:
                    db[VIDEOS_COL].insert_one(v)
                    inserted += 1
                except DuplicateKeyError:
                    pass
                except Exception:
                    pass

            total_new += inserted
            details.append({"channel_id": ch_id, "inserted": inserted})
        except Exception as e:
            details.append({"channel_id": ch_id, "error": str(e)})

    return {"ok": True, "channels": len(channels), "total_new": total_new, "details": details}


# -----------------------------
# Transcript + Shorts Planner
# -----------------------------
def _fetch_transcript_with_timestamps(video_id: str) -> List[Dict[str, Any]]:
    """
    NOTE: On Render, this may hit 429 due to Google bot protection.
    Prefer /transcript/ingest for stable pipeline.
    """
    try:
        return YouTubeTranscriptApi.get_transcript(video_id, languages=["en", "en-US"])
    except Exception:
        return YouTubeTranscriptApi.get_transcript(video_id)


def _text_join(items: List[Dict[str, Any]]) -> str:
    return " ".join([x.get("text", "").replace("\n", " ").strip() for x in items]).strip()


def _build_short_segments(transcript: List[Dict[str, Any]], max_segments: int = 6) -> List[Dict[str, Any]]:
    segments: List[Dict[str, Any]] = []
    if not transcript:
        return segments

    i = 0
    while i < len(transcript) and len(segments) < max_segments:
        start = float(transcript[i].get("start", 0.0))
        end_target = start + 52.0

        chunk = []
        j = i
        while j < len(transcript):
            s = float(transcript[j].get("start", 0.0))
            if s > end_target:
                break
            chunk.append(transcript[j])
            j += 1

        text = _text_join(chunk)
        text_clean = re.sub(r"\s+", " ", text).strip()

        if len(text_clean) >= 80:
            last = chunk[-1]
            end = float(last.get("start", 0.0)) + float(last.get("duration", 0) or 0)
            hook = text_clean[:160]
            score = min(100, 60 + len(text_clean) // 20)

            segments.append(
                {
                    "start_sec": round(start, 2),
                    "end_sec": round(end, 2),
                    "hook": hook,
                    "title_suggestion": hook[:70],
                    "score": int(score),
                }
            )

        i = max(i + 1, j)

    return segments


def _text_to_pseudo_timestamps(text: str, wps: float = 2.6) -> List[Dict[str, Any]]:
    """
    Transcript text -> pseudo timestamp items so we can reuse _build_short_segments().
    wps: average words/sec.
    """
    text = _normalize_text(text)
    if not text:
        return []

    parts = re.split(r"(?<=[.!?])\s+", text)
    parts = [p.strip() for p in parts if p.strip()]
    if not parts:
        parts = [text]

    items: List[Dict[str, Any]] = []
    t = 0.0
    wps = max(1.0, float(wps or 2.6))

    for p in parts:
        words = max(1, len(p.split()))
        dur = max(1.2, words / wps)
        items.append({"text": p, "start": t, "duration": dur})
        t += dur

    return items


def _save_transcript_doc(video_id: str, channel_id: Optional[str], transcript_items: List[Dict[str, Any]], raw_text: str, source: str, language: str):
    now = _utcnow()
    doc = {
        "video_id": video_id,
        "channel_id": channel_id,
        "fetched_at": now,
        "source": source,
        "language": language,
        "items": transcript_items,
        "text": _normalize_text(raw_text)[:400000],
        "updated_at": now,
    }
    db[TRANSCRIPTS_COL].update_one({"video_id": video_id}, {"$set": doc}, upsert=True)


def _create_or_update_plan(video_id: str, max_segments: int = 6) -> Dict[str, Any]:
    if db is None:
        return {"ok": False, "error": "DB not ready"}

    v = db[VIDEOS_COL].find_one({"video_id": video_id}, {"_id": 0})
    if not v:
        return {"ok": False, "error": "video_id not found in DB (scan first)"}

    try:
        transcript = _fetch_transcript_with_timestamps(video_id)
    except Exception as e:
        return {"ok": False, "error": f"transcript fetch failed: {str(e)}"}

    _save_transcript_doc(
        video_id=video_id,
        channel_id=v.get("channel_id"),
        transcript_items=transcript,
        raw_text=_text_join(transcript),
        source="youtube_transcript_api",
        language="auto",
    )

    segments = _build_short_segments(transcript, max_segments=max_segments)

    plan_doc = {
        "video_id": video_id,
        "channel_id": v.get("channel_id"),
        "video_url": v.get("video_url"),
        "source_title": v.get("title"),
        "created_at": _utcnow(),
        "segments": segments,
    }
    db[SHORTS_PLANS_COL].update_one({"video_id": video_id}, {"$set": plan_doc}, upsert=True)

    new_status = "planned" if len(segments) > 0 else "no_segments"
    db[VIDEOS_COL].update_one(
        {"video_id": video_id},
        {"$set": {"status": new_status, "updated_at": _utcnow()}},
    )

    return {"ok": True, "video_id": video_id, "count": len(segments), "segments": segments, "status": new_status}


def _create_or_update_plan_from_text(video_id: str, transcript_text: str, max_segments: int = 6, wps: float = 2.6, language: str = "en", source: str = "manual"):
    if db is None:
        return {"ok": False, "error": "DB not ready"}

    max_segments = max(1, min(int(max_segments or 6), 12))
    wps = max(1.0, float(wps or 2.6))

    v = db[VIDEOS_COL].find_one({"video_id": video_id}, {"_id": 0})
    if not v:
        return {"ok": False, "error": "video_id not found in DB (scan first)"}

    raw_text = _normalize_text(transcript_text)
    if not raw_text:
        return {"ok": False, "error": "transcript_text is empty"}

    pseudo_items = _text_to_pseudo_timestamps(raw_text, wps=wps)

    # Save transcript doc (ingested)
    _save_transcript_doc(
        video_id=video_id,
        channel_id=v.get("channel_id"),
        transcript_items=pseudo_items,
        raw_text=raw_text,
        source=source,
        language=language,
    )

    segments = _build_short_segments(pseudo_items, max_segments=max_segments)

    plan_doc = {
        "video_id": video_id,
        "channel_id": v.get("channel_id"),
        "video_url": v.get("video_url"),
        "source_title": v.get("title"),
        "created_at": _utcnow(),
        "segments": segments,
        "plan_source": "text_ingest",
        "wps": wps,
    }
    db[SHORTS_PLANS_COL].update_one({"video_id": video_id}, {"$set": plan_doc}, upsert=True)

    new_status = "planned_from_text" if len(segments) > 0 else "no_segments"
    db[VIDEOS_COL].update_one(
        {"video_id": video_id},
        {"$set": {"status": new_status, "updated_at": _utcnow()}},
    )

    return {"ok": True, "video_id": video_id, "count": len(segments), "segments": segments, "status": new_status}


# -----------------------------
# Startup / Shutdown
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

        # normalize missing active flag
        try:
            db[CHANNELS_COL].update_many(
                {"active": {"$exists": False}},
                {"$set": {"active": True, "updated_at": _utcnow()}},
            )
        except Exception:
            pass

    except Exception as e:
        print("❌ Mongo connection failed:", e)
        db = None
        return

    scheduler = BackgroundScheduler(timezone="UTC")
    scheduler.add_job(
        lambda: _scan_all_channels(limit_per_channel=5),
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


@app.get("/channels")
def list_channels():
    if db is None:
        return {"ok": False, "error": "DB not ready"}
    items = list(db[CHANNELS_COL].find({}, {"_id": 0}).sort("created_at", -1))
    return {"ok": True, "count": len(items), "items": items}


@app.get("/add-channel")
def add_channel(channel_url: str, active: bool = True):
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
                "$set": {
                    "channel_url": channel_url.strip(),
                    "active": bool(active),
                    "updated_at": now,
                },
                "$setOnInsert": {"created_at": now},
            },
            upsert=True,
        )

        return {"ok": True, "channel_id": channel_id, "saved_url": channel_url.strip(), "active": bool(active)}

    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/scan-channels")
def scan_channels(limit_per_channel: int = 5):
    return _scan_all_channels(limit_per_channel=limit_per_channel)


@app.get("/run-once")
def run_once(limit_per_channel: int = 5, auto_plan: bool = False, max_videos_to_plan: int = 3, max_segments: int = 6):
    """
    Scan channels now. Optionally plan newest videos (NOTE: planning via YouTubeTranscriptApi may hit 429 on Render).
    For stable planning, use /transcript/ingest or /shorts/plan-from-text.
    """
    result = _scan_all_channels(limit_per_channel=limit_per_channel)

    planned = []
    if db is not None and auto_plan and result.get("ok"):
        max_videos_to_plan = max(1, min(int(max_videos_to_plan or 3), 20))
        max_segments = max(1, min(int(max_segments or 6), 12))

        newest = list(
            db[VIDEOS_COL]
            .find({"status": "new"}, {"_id": 0, "video_id": 1})
            .sort("created_at", -1)
            .limit(max_videos_to_plan)
        )

        for x in newest:
            vid = x["video_id"]
            planned.append(_create_or_update_plan(vid, max_segments=max_segments))

    return {"ok": True, "scan": result, "auto_plan": bool(auto_plan), "planned": planned}


@app.get("/videos")
def list_videos(channel_id: Optional[str] = None, limit: int = 50, status: Optional[str] = None):
    if db is None:
        return {"ok": False, "error": "DB not ready"}

    limit = max(1, min(int(limit or 50), 200))
    q: Dict[str, Any] = {}
    if channel_id:
        q["channel_id"] = channel_id
    if status:
        q["status"] = status

    items = list(db[VIDEOS_COL].find(q, {"_id": 0}).sort("created_at", -1).limit(limit))
    return {"ok": True, "count": len(items), "items": items}


@app.get("/transcript/upload")
def transcript_upload(video_id: str, also_plan: bool = True, max_segments: int = 6):
    """
    Tries to fetch transcript via youtube_transcript_api from server side.
    May fail with 429 on cloud. Prefer /transcript/ingest for stable.
    """
    if db is None:
        return {"ok": False, "error": "DB not ready"}

    max_segments = max(1, min(int(max_segments or 6), 12))

    if also_plan:
        return _create_or_update_plan(video_id, max_segments=max_segments)

    v = db[VIDEOS_COL].find_one({"video_id": video_id}, {"_id": 0})
    if not v:
        return {"ok": False, "error": "video_id not found in DB (scan first)"}

    try:
        transcript = _fetch_transcript_with_timestamps(video_id)
    except Exception as e:
        return {"ok": False, "error": f"transcript fetch failed: {str(e)}"}

    _save_transcript_doc(
        video_id=video_id,
        channel_id=v.get("channel_id"),
        transcript_items=transcript,
        raw_text=_text_join(transcript),
        source="youtube_transcript_api",
        language="auto",
    )

    db[VIDEOS_COL].update_one(
        {"video_id": video_id},
        {"$set": {"status": "transcripted", "updated_at": _utcnow()}},
    )

    return {"ok": True, "video_id": video_id, "transcript_items": len(transcript)}


# -----------------------------
# ✅ NEW: ingest transcript text (bypass 429)
# -----------------------------
@app.post("/transcript/ingest")
def transcript_ingest(payload: TranscriptIngestRequest):
    """
    POST JSON:
    {
      "video_id": "KaUuPNYHFNg",
      "transcript_text": "....",
      "source": "browser",
      "also_plan": true,
      "max_segments": 6
    }
    """
    if db is None:
        return {"ok": False, "error": "DB not ready"}

    video_id = payload.video_id.strip()
    text = payload.transcript_text or ""
    source = (payload.source or "manual").strip()
    language = (payload.language or "en").strip()
    max_segments = max(1, min(int(payload.max_segments or 6), 12))

    v = db[VIDEOS_COL].find_one({"video_id": video_id}, {"_id": 0})
    if not v:
        return {"ok": False, "error": "video_id not found in DB (scan first)"}

    raw_text = _normalize_text(text)
    if not raw_text:
        return {"ok": False, "error": "transcript_text is empty"}

    # Save transcript as text (with pseudo timestamps)
    pseudo = _text_to_pseudo_timestamps(raw_text, wps=2.6)
    _save_transcript_doc(
        video_id=video_id,
        channel_id=v.get("channel_id"),
        transcript_items=pseudo,
        raw_text=raw_text,
        source=source,
        language=language,
    )

    # Mark transcripted
    db[VIDEOS_COL].update_one(
        {"video_id": video_id},
        {"$set": {"status": "transcripted_from_text", "updated_at": _utcnow()}},
    )

    if payload.also_plan:
        plan = _create_or_update_plan_from_text(
            video_id=video_id,
            transcript_text=raw_text,
            max_segments=max_segments,
            wps=2.6,
            language=language,
            source=source,
        )
        return {"ok": True, "ingested": True, "planned": True, "plan": plan}

    return {"ok": True, "ingested": True, "planned": False, "video_id": video_id}


# -----------------------------
# ✅ NEW: plan directly from text (stateless-ish)
# -----------------------------
@app.post("/shorts/plan-from-text")
def shorts_plan_from_text(payload: PlanFromTextRequest):
    """
    POST JSON:
    {
      "video_id": "KaUuPNYHFNg",
      "transcript_text": "....",
      "max_segments": 6,
      "wps": 2.6
    }
    """
    if db is None:
        return {"ok": False, "error": "DB not ready"}

    video_id = payload.video_id.strip()
    text = payload.transcript_text or ""
    max_segments = max(1, min(int(payload.max_segments or 6), 12))
    wps = max(1.0, float(payload.wps or 2.6))
    language = (payload.language or "en").strip()
    source = (payload.source or "manual").strip()

    return _create_or_update_plan_from_text(
        video_id=video_id,
        transcript_text=text,
        max_segments=max_segments,
        wps=wps,
        language=language,
        source=source,
    )


@app.get("/shorts/plan")
def shorts_plan(video_id: str, max_segments: int = 6):
    max_segments = max(1, min(int(max_segments or 6), 12))
    return _create_or_update_plan(video_id, max_segments=max_segments)


@app.get("/shorts/plans")
def shorts_plans(video_id: str):
    if db is None:
        return {"ok": False, "error": "DB not ready"}

    doc = db[SHORTS_PLANS_COL].find_one({"video_id": video_id}, {"_id": 0})
    if not doc:
        return {"ok": False, "error": "no plan yet. call /shorts/plan or /shorts/plan-from-text first"}
    return {"ok": True, "data": doc}


@app.get("/transcript/get")
def transcript_get(video_id: str):
    if db is None:
        return {"ok": False, "error": "DB not ready"}

    doc = db[TRANSCRIPTS_COL].find_one({"video_id": video_id}, {"_id": 0})
    if not doc:
        return {"ok": False, "error": "no transcript yet. call /transcript/ingest or /transcript/upload first"}
    return {"ok": True, "data": doc}
