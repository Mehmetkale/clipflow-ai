import os
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
from datetime import datetime

@app.post("/mongo-seed")
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
    @app.get("/mongo-videos")
def mongo_videos():
    if db is None:
        return {"ok": False, "error": "DB not ready"}

    col = db["videos"]
    items = list(col.find({}, {"_id": 0}))

    return {
        "ok": True,
        "count": len(items),
        "items": items
    }
