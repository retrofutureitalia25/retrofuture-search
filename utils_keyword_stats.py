# utils_keyword_stats.py

from datetime import datetime, UTC
from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")

DB_NAME = "database_vintage"
COLLECTION = "keyword_stats"


# --------------------------------------------------
# Connessione DB
# --------------------------------------------------

def get_collection():

    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    return db[COLLECTION]


# --------------------------------------------------
# registra query utente
# --------------------------------------------------

def register_query(term: str):

    if not term:
        return

    term = term.lower().strip()

    col = get_collection()

    col.update_one(
        {"term": term},
        {
            "$inc": {"query_freq": 1},
            "$set": {"last_seen": datetime.now(UTC)}
        },
        upsert=True
    )


# --------------------------------------------------
# registra click utente
# --------------------------------------------------

def register_click(term: str):

    if not term:
        return

    term = term.lower().strip()

    col = get_collection()

    col.update_one(
        {"term": term},
        {
            "$inc": {"click_freq": 1},
            "$set": {"last_clicked": datetime.now(UTC)}
        },
        upsert=True
    )
