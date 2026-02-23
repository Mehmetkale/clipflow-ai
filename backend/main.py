import os
import requests
from datetime import datetime
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient
from pymongo.server_api import ServerApi

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

mongo_client = None
db = None

@app.on_event("startup")
def startup_db():
    global mongo_client, db
    mongodb_uri = os.getenv("MONGODB_URI")

    if not mongodb_uri:
        print("❌ MONGODB_URI not found")
        return

    mongo_client = MongoClient(
        mongodb_uri,
        server_api=ServerApi("1"),
        serverSelectionTimeoutMS=10000
    )

    try:
        mongo_client.admin.command("ping")
        db = mongo_client["clipflow_db"]
        print("✅ MongoDB Connected Successfully")
    except Exception as e:
        print("❌ Mongo Connection Failed:", e)


@app.get("/")
def root():
    return {"message": "ClipFlow AI Backend Running 🚀"}


@app.get("/mongo-test")
def mongo_test():
    if db is None:
        return {"ok": False, "error": "DB not ready"}
    return {"ok": True, "collections": db.list_collection_names()}


@app.get("/mongo-seed")
def mongo_seed():
    if db is None:
        return {"ok": False, "error": "DB not ready"}

    col = db["videos"]

    doc = {
        "channel_id": "TEST_CHANNEL",
        "video_id": "TEST_VIDEO",
        "title": "Hello Mongo",
        "created_at": datetime.utcnow(),
        "status": "new"
    }

    result = col.insert_one(doc)

    return {
        "ok": True,
        "inserted_id": str(result.inserted_id)
    }


# 🔥 YENİ ENDPOINT
@app.get("/add-channel")
def add_channel(channel_url: str):
    if db is None:
        return {"ok": False, "error": "DB not ready"}

    api_key = os.getenv("YOUTUBE_API_KEY")
    if not api_key:
        return {"ok": False, "error": "YOUTUBE_API_KEY not set"}

    # Handle URL'den username çıkar
    if "@" in channel_url:
        username = channel_url.split("@")[-1]
    else:
        return {"ok": False, "error": "Invalid channel URL"}

    youtube_api = f"https://www.googleapis.com/youtube/v3/channels?part=id&forHandle={username}&key={api_key}"

    response = requests.get(youtube_api)
    data = response.json()

    if "items" not in data or len(data["items"]) == 0:
        return {"ok": False, "error": "Channel not found"}

    channel_id = data["items"][0]["id"]

    col = db["channels"]

    col.insert_one({
        "channel_id": channel_id,
        "channel_url": channel_url,
        "created_at": datetime.utcnow(),
        "active": True
    })

    return {
        "ok": True,
        "channel_id": channel_id,
        "saved_url": channel_url
    }
