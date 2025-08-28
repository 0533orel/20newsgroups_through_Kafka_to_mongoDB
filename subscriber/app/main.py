from fastapi import FastAPI, Query
from kafka import KafkaConsumer
from pymongo import MongoClient, ASCENDING
from datetime import datetime
import os, json, threading

# ========= 1) הגדרות בסיס =========
# (נלקחות מ-ENV בקונטיינר; יש ברירות מחדל שמתאימות ל-compose שלך)
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC_NAME       = os.getenv("TOPIC_NAME", "interesting")          # לכל צרכן יהיה ערך שונה
GROUP_ID         = os.getenv("CONSUMER_GROUP", "sub_group")        # קבוצה שונה לכל צרכן
MONGO_URI        = os.getenv("MONGO_URI", "mongodb://mongo:27017")
MONGO_DB         = os.getenv("MONGO_DB", "kafka20news")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "messages")

# ========= 2) חיבור ל-MongoDB =========
mongo = MongoClient(MONGO_URI)
col = mongo[MONGO_DB][MONGO_COLLECTION]
# אינדקס לפי זמן קליטה — לא חובה, אבל עוזר לשאילתות /messages
col.create_index([("ts_received", ASCENDING)], name="ts_received_idx", background=True)

# ========= 3) משתנה מצב ל-"מה חדש מאז" =========
# נשמור כאן את חותמת הזמן האחרונה שהחזרנו ב-/messages
_last_seen_ts: datetime | None = None

# ========= 4) פונקציה: לולאת הצרכן =========
def consumer_loop():
    """
    מאזין ל- Kafka ברקע.
    כל הודעה שנקלטת: מוסיפים ts_received ושומרים למסד.
    """
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=GROUP_ID,
        enable_auto_commit=True,           # שומר התקדמות (offsets) אוטומטית
        auto_offset_reset="earliest",      # אם אין offset שמור – להתחיל מההתחלה
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=0              # ריצה רציפה (לא נזרק StopIteration)
    )
    for record in consumer:
        try:
            doc = record.value                 # זו המפה שה-Publisher שלח
            # מוסיפים מידע מקומי:
            doc["ts_received"]   = datetime.utcnow()   # חותמת זמן קבלה (UTC)
            doc["kafka_partition"] = record.partition  # לא חובה, טוב לדיבוג
            doc["kafka_offset"]    = record.offset
            col.insert_one(doc)
        except Exception as e:
            # שקט: אפשר להדפיס/לשמור לוג אם תרצה להבין תקלות
            print("consume error:", e)

# מרימים את המאזין ברקע כשיורד הקובץ (פשוט וברור)
t = threading.Thread(target=consumer_loop, daemon=True)
t.start()

# ========= 5) אפליקציית FastAPI =========
app = FastAPI(title=f"Subscriber for '{TOPIC_NAME}'")

@app.get("/health")
def health():
    return {
        "status": "ok",
        "topic": TOPIC_NAME,
        "bootstrap": KAFKA_BOOTSTRAP,
        "group_id": GROUP_ID,
        "mongo_collection": MONGO_COLLECTION
    }

def _serialize(doc: dict) -> dict:
    """המרה נעימה ל-JSON: ObjectId -> str, datetime -> isoformat"""
    d = dict(doc)
    if "_id" in d:
        d["_id"] = str(d["_id"])
    if isinstance(d.get("ts_received"), datetime):
        d["ts_received"] = d["ts_received"].isoformat()
    return d

@app.get("/messages")
def get_new_messages(limit: int = Query(20, ge=1, le=1000)):
    """
    מחזיר רק את ההודעות שהגיעו "מאז הפעם האחרונה" שקראת ל- /messages.
    בקריאה ראשונה יחזיר את כל מה שיש עד כה (עד limit),
    ואז יעדכן את החותמת לפעם הבאה.
    """
    global _last_seen_ts

    # בונים פילטר לפי החותמת האחרונה
    query = {}
    if _last_seen_ts is not None:
        query = {"ts_received": {"$gt": _last_seen_ts}}

    # מושכים לפי סדר כרונולוגי, עד limit
    docs = list(col.find(query).sort("ts_received", ASCENDING).limit(limit))

    # אם מצאנו משהו — מעדכנים את החותמת לחדשה ביותר שהחזרנו
    if docs:
        _last_seen_ts = docs[-1]["ts_received"]

    # ממירים לפורמט ידידותי ל-JSON
    return {
        "count": len(docs),
        "items": [_serialize(d) for d in docs]
    }

@app.get("/messages/all")
def get_all_messages(limit: int = Query(50, ge=1, le=5000)):
    """
    כלי בקרה/בדיקות: מחזיר כל מה שיש (עד limit), לפי סדר זמן.
    לא חלק מהדרישה ההכרחית, אבל עוזר לראות מה נקלט בפועל.
    """
    docs = list(col.find({}).sort("ts_received", ASCENDING).limit(limit))
    return {
        "count": len(docs),
        "items": [_serialize(d) for d in docs]
    }
