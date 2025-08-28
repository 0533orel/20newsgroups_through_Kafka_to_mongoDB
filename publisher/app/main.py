from fastapi import FastAPI
from datetime import datetime
from kafka import KafkaProducer
from sklearn.datasets import fetch_20newsgroups
import json, os, random

app = FastAPI(title="20newsgroups Publisher - Simple")

# ---- הגדרות בסיס ----
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC_INTERESTING = "interesting"
TOPIC_NOT_INTERESTING = "not_interesting"

interesting_categories = [
    'alt.atheism', 'comp.graphics', 'comp.os.ms-windows.misc', 'comp.sys.ibm.pc.hardware',
    'comp.sys.mac.hardware', 'comp.windows.x', 'misc.forsale', 'rec.autos',
    'rec.motorcycles', 'rec.sport.baseball'
]

not_interesting_categories = [
    'rec.sport.hockey', 'sci.crypt', 'sci.electronics', 'sci.med', 'sci.space',
    'soc.religion.christian', 'talk.politics.guns', 'talk.politics.mideast',
    'talk.politics.misc', 'talk.religion.misc'
]

# ---- טוענים פעם אחת את הדאטהסט לכל ה-20 קטגוריות ----
all_categories = interesting_categories + not_interesting_categories
ds = fetch_20newsgroups(subset='all', categories=all_categories)

# בונים מיפוי פשוט: קטגוריה -> רשימת אינדקסים במסד הנתונים
cat_to_indices = {c: [] for c in all_categories}
for i, target in enumerate(ds.target):
    cat = ds.target_names[target]
    if cat in cat_to_indices:
        cat_to_indices[cat].append(i)

# ---- יצרן Kafka פשוט ----
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
)

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/publish")
def publish():
    """
    בכל קריאה: שולח 20 הודעות:
    - 10 הודעות (אחת מכל קטגוריה) ל-topic 'interesting'
    - 10 הודעות (אחת מכל קטגוריה) ל-topic 'not_interesting'
    בוחרים דוגמה אקראית לכל קטגוריה כדי לשמור על פשטות.
    """
    sent_i = 0
    sent_n = 0
    now_iso = datetime.utcnow().isoformat()

    # 10 "מעניינות"
    for cat in interesting_categories:
        idx_list = cat_to_indices.get(cat, [])
        if not idx_list:
            continue
        idx = random.choice(idx_list)  # בחירה אקראית פשוטה
        msg = {
            "dataset": "20newsgroups",
            "category": cat,
            "text": ds.data[idx],
            "source_idx": idx,
            "ts_pub": now_iso,
        }
        producer.send(TOPIC_INTERESTING, msg)
        sent_i += 1

    # 10 "לא מעניינות"
    for cat in not_interesting_categories:
        idx_list = cat_to_indices.get(cat, [])
        if not idx_list:
            continue
        idx = random.choice(idx_list)
        msg = {
            "dataset": "20newsgroups",
            "category": cat,
            "text": ds.data[idx],
            "source_idx": idx,
            "ts_pub": now_iso,
        }
        producer.send(TOPIC_NOT_INTERESTING, msg)
        sent_n += 1

    producer.flush()

    return {
        "published": {"interesting": sent_i, "not_interesting": sent_n},
        "total": sent_i + sent_n,
        "ts": now_iso,
    }
