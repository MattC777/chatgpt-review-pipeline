#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# This script runs daily in CI to produce 3 topic tables in Snowflake:
# ANALYTICS.TOPICS_KEYWORDS / TOPICS_EXAMPLES / TOPICS_SUMMARY
# It reads from ANALYTICS.V_LOW_SCORE_CLEAN (already created by you).

import os, re, numpy as np, pandas as pd
from datetime import datetime
import snowflake.connector
from snowflake.connector import connect
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import NMF
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction import text as sk_text

# ---------- Config (read from GitHub Actions env/secrets) ----------
DB_NAME       = os.environ.get("SNOWFLAKE_DATABASE", "REVIEWS_PIPELINE")
SCHEMA_NAME   = os.environ.get("SNOWFLAKE_SCHEMA",   "ANALYTICS")   # <- 在 workflow 的分析步骤里注入 ANALYTICS
VIEW_NAME     = "V_LOW_SCORE_CLEAN"

TABLE_TOPICS   = "TOPICS_KEYWORDS"
TABLE_EXAMPLES = "TOPICS_EXAMPLES"
TABLE_SUMMARY  = "TOPICS_SUMMARY"

# Topic modeling hyper-params (kept from your notebook)
K_RANGE          = range(6, 13)      # choose from 6..12
TOPN_WORDS       = 12
EXAMPLES_PER_TP  = 5
MIN_DF           = 12
MAX_DF           = 0.40
MAX_FEATURES     = 50000
EN_PROP_MIN      = 0.60
MMR_LAMBDA       = 0.7
DEDUP_JACCARD    = 0.90

# Domain stopwords & canonicalization
DOMAIN_STOP = {
    "app","apps","chatgpt","openai","ai","gpt","version","versions","update","updated",
    "fix","fixed","issue","issues","problem","problems","bug","bugs","please","pls",
    "thanks","thank","hi","hello","team","dear","experience","experiences",
    "nice","good","bad","very","best","worst","useful","useless","helpful","not",
    "really","actually","also","ever","always","never","still","just","well","ok",
    "work","works","working","worked","proper","properly","application"
}
STOP_WORDS = list(sk_text.ENGLISH_STOP_WORDS.union(DOMAIN_STOP))

CANON_MAP = {
    r"\blog in\b": "login", r"\blogin\b": "login", r"\bsign in\b": "login",
    r"\b2fa\b": "mfa", r"\b2-factor\b": "mfa", r"\b2 factor\b": "mfa",
    r"\bslow\b": "lag", r"\blaggy\b": "lag", r"\blag\b": "lag",
    r"\bprice\b": "pricing", r"\bcharged?\b": "billing",
    r"\brefunds?\b": "refund", r"\bsubscription\b": "subscribe",
    r"\bcrash(es|ed|ing)?\b": "crash",
    r"\bdoesn['’]?t work\b": "not_working",
    r"\bisn['’]?t working\b": "not_working",
    r"\bnot working\b": "not_working",
}

NAME_RULES = [
    ("Login / Account",       ["login","account","password","otp","mfa","verification"]),
    ("Billing / Subscription",["billing","subscribe","payment","pricing","refund","charge"]),
    ("Performance / Crash",   ["lag","slow","freeze","loading","crash","latency"]),
    ("Answer Quality",        ["answer","response","quality","accuracy","wrong","hallucination"]),
    ("Access / Region",       ["region","country","available","access","blocked","unsupported"]),
    ("UI / Usability",        ["ui","ux","button","menu","dark mode","layout"]),
]

# ---------- Snowflake connection ----------
def connect_sf():
    cfg = {
        "user":      os.environ["SNOWFLAKE_USER"],
        "password":  os.environ["SNOWFLAKE_PASSWORD"],
        "account":   os.environ["SNOWFLAKE_ACCOUNT"],
        "role":      os.environ.get("SNOWFLAKE_ROLE", "SYSADMIN"),
        "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        "database":  DB_NAME,
        "schema":    SCHEMA_NAME,
    }
    conn = connect(**cfg)
    with conn.cursor() as cs:
        cs.execute(f"USE ROLE {cfg['role']}")
        cs.execute(f"USE WAREHOUSE {cfg['warehouse']}")
        cs.execute(f"USE DATABASE {DB_NAME}")
        cs.execute(f"USE SCHEMA {SCHEMA_NAME}")
    return conn

def fetch_df(conn):
    sql = f"""
      SELECT review_id, content, thumbs_up_count, app_version, review_time
      FROM {DB_NAME}.{SCHEMA_NAME}.{VIEW_NAME}
    """
    df = pd.read_sql(sql, conn)
    df.columns = [c.lower() for c in df.columns]
    return df

# ---------- Cleaning ----------
def english_prop(s: str) -> float:
    s = str(s or "")
    letters = sum(ch.isalpha() for ch in s)
    en_letters = sum('a' <= ch.lower() <= 'z' for ch in s)
    return (en_letters / letters) if letters else 0.0

def canon_replace(text: str) -> str:
    t = text
    for pat, rep in CANON_MAP.items(): t = re.sub(pat, rep, t)
    return t

def clean_text(s: str) -> str:
    t = str(s or "").lower()
    t = re.sub(r"http\S+|www\S+", " ", t)
    t = canon_replace(t)
    t = re.sub(r"[^a-z0-9\s']", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t

# ---------- Similarity / MMR ----------
def _tokens_for_jaccard(text: str):
    return {w for w in re.findall(r"[a-zA-Z]{2,}", (text or "").lower()) if w not in STOP_WORDS}

def jaccard(a: set, b: set) -> float:
    if not a or not b: return 0.0
    return len(a & b) / len(a | b)

def mmr_select(texts, rel_scores, top_k, lam=MMR_LAMBDA):
    chosen, cand = [], list(range(len(texts)))
    token_sets = [_tokens_for_jaccard(t) for t in texts]
    if not cand: return []
    first = int(np.argmax(rel_scores)); chosen.append(first); cand.remove(first)
    while len(chosen) < min(top_k, len(texts)) and cand:
        best_i, best_score = None, -1e9
        for i in cand:
            sim_to_chosen = 0.0 if not chosen else max(jaccard(token_sets[i], token_sets[j]) for j in chosen)
            score = lam * rel_scores[i] - (1 - lam) * sim_to_chosen
            if score > best_score: best_score, best_i = score, i
        chosen.append(best_i); cand.remove(best_i)
    return chosen

# ---------- Topic naming ----------
def _is_english_phrase(s: str) -> bool:
    return bool(re.fullmatch(r"[A-Za-z]+(?: [A-Za-z]+)*", s or ""))

def auto_name(keywords: list[str]) -> str:
    kw_join = " ".join(keywords)
    for name, needles in NAME_RULES:
        if any(n in kw_join for n in needles): return name
    eng = [w for w in keywords if _is_english_phrase(w)]
    return ", ".join(eng[:3]).title() if eng else "General"

# ---------- Model fitting ----------
def score_topics(H):
    sim = cosine_similarity(H); np.fill_diagonal(sim, 0.0)
    inter_sim = sim.mean()
    sparsity = (H < (H.mean(axis=1, keepdims=True))).mean()
    return (1 - inter_sim) * 0.6 + sparsity * 0.4

def fit_nmf_with_best_k(X):
    best = None
    for k in K_RANGE:
        nmf = NMF(n_components=k, init="nndsvda", random_state=42, max_iter=400)
        W = nmf.fit_transform(X); H = nmf.components_
        s = score_topics(H)
        if (best is None) or (s > best["score"]): best = {"k": k, "model": nmf, "W": W, "H": H, "score": s}
    return best

# ---------- Main topic pipeline ----------
def topic_pipeline(df: pd.DataFrame):
    df = df.copy()
    for col in ["content","review_id","thumbs_up_count","app_version","review_time"]:
        if col not in df.columns: raise KeyError(f"missing column: {col}")

    df["en_prop"] = df["content"].apply(english_prop)
    df = df[df["en_prop"] >= EN_PROP_MIN]
    df["text_clean"] = df["content"].apply(clean_text)
    df = df[df["text_clean"].str.len() >= 10].reset_index(drop=True)

    if df.empty: 
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    vec = TfidfVectorizer(
        ngram_range=(1,3),
        min_df=MIN_DF, max_df=MAX_DF, max_features=MAX_FEATURES,
        stop_words=STOP_WORDS, sublinear_tf=True,
        token_pattern=r'(?u)\b[a-zA-Z]{2,}\b'
    )
    X = vec.fit_transform(df["text_clean"])
    terms = vec.get_feature_names_out()
    if X.shape[0] < min(K_RANGE):
        raise ValueError(f"Too few documents ({X.shape[0]}) for topic modeling.")

    best = fit_nmf_with_best_k(X)
    nmf, W, H, k = best["model"], best["W"], best["H"], best["k"]
    print(f"[Info] Selected K={k} (score={best['score']:.3f})")

    # Topic keywords
    topics_rows, name_map = [], {}
    for t_idx in range(k):
        idx = np.argsort(H[t_idx])[::-1][:TOPN_WORDS]
        cand = [terms[i] for i in idx]
        kws_eng = [w for w in cand if _is_english_phrase(w) and w.lower() not in STOP_WORDS][:TOPN_WORDS]
        topics_rows.append({"topic": int(t_idx), "keywords": ", ".join(kws_eng)})
        name_map[t_idx] = auto_name(kws_eng)
    topics_df = pd.DataFrame(topics_rows)

    # Representative examples
    df["topic"] = W.argmax(axis=1)
    rows = []
    for t_idx in range(k):
        sub = df[df["topic"] == t_idx].copy()
        if sub.empty: continue
        pos = df.index.get_indexer(sub.index)
        weights = W[pos, t_idx]
        rep = weights * np.log1p(sub["thumbs_up_count"].fillna(0))
        sub = sub.assign(rep=rep)

        # deduplicate
        uniq, seen = [], []
        for _, r in sub.sort_values("rep", ascending=False).iterrows():
            toks = _tokens_for_jaccard(r["content"])
            if any(jaccard(toks, s) >= DEDUP_JACCARD for s in seen): 
                continue
            seen.append(toks); uniq.append(r)
        if not uniq: continue

        texts = [str(r["content"]) for r in uniq]
        rels  = [float(r["rep"]) for r in uniq]
        picks = mmr_select(texts, rels, EXAMPLES_PER_TP, lam=MMR_LAMBDA)
        for j in picks:
            r = uniq[j]
            rows.append({
                "topic": int(t_idx),
                "topic_name": name_map[t_idx],
                "review_id": r["review_id"],
                "thumbs_up_count": int(r.get("thumbs_up_count") or 0),
                "app_version": r.get("app_version"),
                "review_time": r.get("review_time"),
                "example_review": r["content"],
                "rep_score": float(r["rep"])
            })
    examples_df = pd.DataFrame(rows)

    # Summary
    total = len(df)
    summary_rows = []
    for t_idx in range(k):
        sub = df[df["topic"] == t_idx]
        if len(sub) == 0: continue
        share = len(sub) / total
        thumbs = sub["thumbs_up_count"].fillna(0)
        summary_rows.append({
            "topic": int(t_idx),
            "topic_name": name_map[t_idx],
            "share_pct": round(share * 100, 2),
            "thumbs_avg": float(thumbs.mean()),
            "thumbs_median": float(thumbs.median()),
            "sample_size": int(len(sub))
        })
    summary_df = pd.DataFrame(summary_rows).sort_values("topic")  

    return topics_df, examples_df, summary_df

# ---------- Tables (ensure & write-back) ----------
def ensure_tables(conn):
    with conn.cursor() as cs:
        cs.execute(f"""
        CREATE TABLE IF NOT EXISTS {DB_NAME}.{SCHEMA_NAME}.{TABLE_TOPICS} (
          run_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
          topic NUMBER,
          keywords STRING
        )""")
        cs.execute(f"""
        CREATE TABLE IF NOT EXISTS {DB_NAME}.{SCHEMA_NAME}.{TABLE_EXAMPLES} (
          run_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
          topic NUMBER,
          review_id STRING,
          thumbs_up_count NUMBER,
          app_version STRING,
          review_time TIMESTAMP_NTZ,
          example_review STRING,
          topic_name STRING,
          rep_score FLOAT
        )""")
        cs.execute(f"""
        CREATE TABLE IF NOT EXISTS {DB_NAME}.{SCHEMA_NAME}.{TABLE_SUMMARY} (
          run_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
          topic NUMBER,
          topic_name STRING,
          share_pct FLOAT,
          thumbs_avg FLOAT,
          thumbs_median FLOAT,
          sample_size NUMBER
        )""")

def to_iso_ntz(x):
    if pd.isna(x): return None
    ts = pd.to_datetime(x, errors="coerce")
    if pd.isna(ts): return None
    try: ts = ts.tz_convert(None)
    except Exception: pass
    return ts.strftime("%Y-%m-%d %H:%M:%S")

def write_back(conn, topics_df, examples_df, summary_df):
    if not examples_df.empty and "review_time" in examples_df.columns:
        examples_df = examples_df.copy()
        examples_df["review_time"] = examples_df["review_time"].apply(to_iso_ntz)

    with conn.cursor() as cs:
        # Idempotent by day: clear today's batch then insert
        for tbl in [TABLE_TOPICS, TABLE_EXAMPLES, TABLE_SUMMARY]:
            cs.execute(f"DELETE FROM {DB_NAME}.{SCHEMA_NAME}.{tbl} WHERE DATE(run_at)=CURRENT_DATE()")

        if not topics_df.empty:
            cs.executemany(
                f"INSERT INTO {DB_NAME}.{SCHEMA_NAME}.{TABLE_TOPICS}(topic, keywords) VALUES (%(topic)s, %(keywords)s)",
                topics_df.to_dict("records"))

        if not examples_df.empty:
            cs.executemany(
                f"""INSERT INTO {DB_NAME}.{SCHEMA_NAME}.{TABLE_EXAMPLES}
                (topic, topic_name, review_id, thumbs_up_count, app_version, review_time, example_review, rep_score)
                VALUES (%(topic)s, %(topic_name)s, %(review_id)s, %(thumbs_up_count)s, %(app_version)s, %(review_time)s, %(example_review)s, %(rep_score)s)""",
                examples_df.to_dict("records"))

        if not summary_df.empty:
            cs.executemany(
                f"""INSERT INTO {DB_NAME}.{SCHEMA_NAME}.{TABLE_SUMMARY}
                (topic, topic_name, share_pct, thumbs_avg, thumbs_median, sample_size)
                VALUES (%(topic)s, %(topic_name)s, %(share_pct)s, %(thumbs_avg)s, %(thumbs_median)s, %(sample_size)s)""",
                summary_df.to_dict("records"))

# ---------- Main ----------
def main():
    conn = connect_sf()

    ctx = pd.read_sql("SELECT CURRENT_USER() u, CURRENT_ROLE() r, CURRENT_DATABASE() db, CURRENT_SCHEMA() sch", conn)
    print(ctx.to_string(index=False))

    df = fetch_df(conn)
    print(f"Loaded {len(df):,} low-score rows (before filters)")

    topics_df, examples_df, summary_df = topic_pipeline(df)

    # optional local CSVs (for debugging in CI artifacts)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    os.makedirs("data", exist_ok=True)
    topics_df.to_csv(f"data/topics_keywords_{ts}.csv", index=False)
    examples_df.to_csv(f"data/topics_examples_{ts}.csv", index=False)
    summary_df.to_csv(f"data/topics_summary_{ts}.csv", index=False)
    print("[SAVE] CSVs under data/ for artifacts")

    ensure_tables(conn)
    write_back(conn, topics_df, examples_df, summary_df)
    print(f"Wrote results to {DB_NAME}.{SCHEMA_NAME}.{TABLE_TOPICS} / {TABLE_EXAMPLES} / {TABLE_SUMMARY}")

    conn.close(); print("Done.")

if __name__ == "__main__":
    main()
