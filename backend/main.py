import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient
from pymongo.server_api import ServerApi

app = FastAPI()

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Globals
mongo_client: MongoClient | None = None
db = None

@app.on_event("startup")
def startup_db():
    global mongo_client, db
    mongodb_uri = os.getenv("MONGODB_URI")
    if not mongodb_uri:
        raise RuntimeError("MONGODB_URI is not set")

    # Atlas için daha stabil bağlantı
    mongo_client = MongoClient(
        mongodb_uri,
        server_api=ServerApi("1"),
        connectTimeoutMS=10000,
        socketTimeoutMS=10000,
        serverSelectionTimeoutMS=10000,
    )
    # bağlantıyı test et
    mongo_client.admin.command("ping")
    db = mongo_client["clipflow_db"]
    print("✅ MongoDB Connected Successfully")

@app.get("/")
def root():
    return {"message": "ClipFlow AI Backend Running 🚀"}

# Render healthcheck bazen HEAD atar; 405 görmemek için:
@app.head("/")
def root_head():
    return

@app.get("/mongo-test")
def mongo_test():
    if db is None:
        return {"ok": False, "error": "DB not ready"}
    collections = db.list_collection_names()
    return {"ok": True, "collections": collections}
