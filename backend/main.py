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
