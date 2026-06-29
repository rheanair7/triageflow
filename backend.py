"""
TriageFlow backend — FastAPI multi-agent incident + support triage.

New in this version:
  - SQLite audit log  (/history)
  - Custom knowledge base  (/kb)
  - Webhook / Slack integration  (/settings)
  - Agent wire SSE events (live message passing between agents)
  - Speed benchmark vs GPU provider  (/benchmark)

Run:
  pip install fastapi uvicorn openai python-multipart
  uvicorn backend:app --reload --port 8000
"""

import os, json, base64, time, sqlite3, re, queue as qmod, tempfile
from threading import Thread, Lock
from contextlib import contextmanager
from urllib import request as urlreq

from fastapi import FastAPI, UploadFile, File, Form, Header, HTTPException, Depends
from fastapi.responses import StreamingResponse, HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import Optional
from openai import OpenAI

client = OpenAI(
    api_key=os.environ["CEREBRAS_API_KEY"],
    base_url="https://api.cerebras.ai/v1",
)

MODEL = "gemma-4-31b"
CONF_RANK = {"low": 0, "medium": 1, "high": 2}

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------
_data_dir = os.environ.get("DATA_DIR", os.path.dirname(__file__) or ".")
DB_PATH = os.path.join(_data_dir, "triageflow.db")
_db_lock = Lock()


def _init_db():
    with sqlite3.connect(DB_PATH) as con:
        con.executescript("""
        CREATE TABLE IF NOT EXISTS triage_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts REAL NOT NULL,
            mode TEXT,
            incident_type TEXT,
            severity TEXT,
            confidence TEXT,
            action_plan TEXT,
            rationale TEXT,
            total_ms REAL,
            filename TEXT
        );
        CREATE TABLE IF NOT EXISTS custom_kb (
            key TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            keywords TEXT NOT NULL,
            content TEXT NOT NULL,
            created_at REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        );
        """)


_init_db()


@contextmanager
def get_db():
    with _db_lock:
        con = sqlite3.connect(DB_PATH)
        con.row_factory = sqlite3.Row
        try:
            yield con
            con.commit()
        finally:
            con.close()


def get_setting(key, default=None):
    with get_db() as con:
        row = con.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        return row["value"] if row else default


def set_setting(key, value):
    with get_db() as con:
        con.execute("INSERT OR REPLACE INTO settings (key,value) VALUES (?,?)", (key, value))


def load_custom_kb():
    with get_db() as con:
        rows = con.execute("SELECT key,title,keywords,content FROM custom_kb").fetchall()
    return {r["key"]: {"title": r["title"], "keywords": json.loads(r["keywords"]), "content": r["content"]}
            for r in rows}


def save_triage_log(mode, triage, resolution, total_ms, filename=""):
    with get_db() as con:
        con.execute(
            "INSERT INTO triage_log (ts,mode,incident_type,severity,confidence,action_plan,rationale,total_ms,filename) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (time.time(), mode,
             triage.get("incident_type", ""), triage.get("severity", ""),
             resolution.get("confidence", ""),
             json.dumps(resolution.get("action_plan", [])),
             resolution.get("rationale", ""), total_ms, filename),
        )


def post_webhook(url, payload):
    if not url:
        return
    data = json.dumps(payload).encode("utf-8")
    req = urlreq.Request(url, data=data, method="POST",
                         headers={"Content-Type": "application/json", "User-Agent": "TriageFlow/1.0"})
    urlreq.urlopen(req, timeout=10)


# ---------------------------------------------------------------------------
# Strict JSON schemas
# ---------------------------------------------------------------------------
TRIAGE_SCHEMA = {
    "type": "object",
    "properties": {
        "incident_type": {"type": "string"},
        "severity": {"type": "string", "enum": ["low", "medium", "high", "critical"]},
        "summary": {"type": "string"},
        "retrieval_query": {"type": "string"},
    },
    "required": ["incident_type", "severity", "summary", "retrieval_query"],
    "additionalProperties": False,
}
RESOLUTION_SCHEMA = {
    "type": "object",
    "properties": {
        "confidence": {"type": "string", "enum": ["low", "medium", "high"]},
        "action_plan": {"type": "array", "items": {"type": "string"}},
        "diagnostic_questions": {"type": "array", "items": {"type": "string"}},
        "rationale": {"type": "string"},
        "draft_reply": {"type": "string"},
    },
    "required": ["confidence", "action_plan", "diagnostic_questions", "rationale", "draft_reply"],
    "additionalProperties": False,
}
DIAGNOSTIC_SCHEMA = {
    "type": "object",
    "properties": {
        "answers": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {"question": {"type": "string"}, "answer": {"type": "string"}},
                "required": ["question", "answer"],
                "additionalProperties": False,
            },
        },
    },
    "required": ["answers"],
    "additionalProperties": False,
}
RETRIEVAL_SCHEMA = {
    "type": "object",
    "properties": {
        "selected_keys": {"type": "array", "items": {"type": "string"}},
        "reasoning": {"type": "string"},
    },
    "required": ["selected_keys", "reasoning"],
    "additionalProperties": False,
}

# ---------------------------------------------------------------------------
# Knowledge bases
# ---------------------------------------------------------------------------
RUNBOOKS = {
    "database_connection": {"title": "Database connection pool exhausted",
        "keywords": ["database", "db", "connection", "pool", "clients", "slots", "postgres", "mysql"],
        "content": "Symptoms: timeouts, 'too many connections'. Fix: check pool max size, find "
                   "connection leaks, scale read replicas, restart the pooler if saturated."},
    "high_latency": {"title": "API latency spike",
        "keywords": ["latency", "slow", "p99", "p95", "response time", "timeout", "degraded"],
        "content": "Symptoms: p99 latency rising. Fix: check downstream health, inspect recent "
                   "deploys, look for N+1 queries, enable caching, check CPU saturation."},
    "disk_full": {"title": "Disk space exhausted",
        "keywords": ["disk", "space", "no space left", "df", "volume", "errno 28", "write failure", "storage"],
        "content": "Symptoms: write failures, no space errors. Fix: clear old logs, rotate logs, "
                   "expand volume, check runaway temp files."},
    "error_rate_spike": {"title": "5xx error rate spike",
        "keywords": ["5xx", "500", "error rate", "error budget", "gateway", "failed requests", "503", "502"],
        "content": "Symptoms: surge in 500s. Fix: roll back last deploy, check exception traces, "
                   "verify upstream health, inspect config changes."},
    "cpu_saturation": {"title": "CPU / resource saturation",
        "keywords": ["cpu", "memory", "saturation", "resource", "throttling", "oom", "garbage collection", "limits"],
        "content": "Symptoms: high CPU/memory, throttling, GC pauses. Fix: check pod limits, find "
                   "hot code paths, scale horizontally, look for leaks, tune GC."},
    "shipment_damage": {"title": "Damaged shipment intake",
        "keywords": ["shipment", "cargo", "package", "damage", "freight", "carrier", "pallet", "box", "crate"],
        "content": "Symptoms: visible package/cargo damage. Fix: document with photos, file a damage "
                   "claim with carrier reference, flag the load ID, notify consignee, quarantine inventory."},
}

SUPPORT_KB = {
    "login_failure": {"title": "Login / authentication failures",
        "keywords": ["login", "log in", "sign in", "password", "authentication", "auth", "locked out", "credentials", "mfa", "2fa"],
        "content": "Common causes: expired password, locked account, MFA device change. "
                   "Resolution: guide password reset, verify identity, unlock account, clear cache/cookies."},
    "payment_billing": {"title": "Payment & billing issues",
        "keywords": ["payment", "billing", "charge", "card", "declined", "invoice", "refund", "subscription", "overcharged"],
        "content": "Common causes: declined card, expired card, duplicate charge. Resolution: verify card "
                   "details, check duplicates, explain billing cycle, issue refund if warranted."},
    "upload_error": {"title": "Upload / file errors",
        "keywords": ["upload", "file", "attachment", "error", "failed", "size limit", "format", "image won't", "document"],
        "content": "Common causes: file too large, unsupported format, network interruption. Resolution: "
                   "confirm size/format limits, suggest supported types, retry on stable connection."},
    "feature_broken": {"title": "Feature not working as expected",
        "keywords": ["not working", "broken", "bug", "glitch", "button", "page", "blank", "error message", "won't load", "frozen"],
        "content": "Common causes: stale cache, browser incompatibility, known bug. Resolution: reproduce "
                   "the issue, check status page, suggest cache clear, collect screenshot + steps."},
    "account_access": {"title": "Account access & permissions",
        "keywords": ["access", "permission", "role", "admin", "can't see", "missing", "seat", "invite", "member", "team"],
        "content": "Common causes: insufficient role, seat not assigned, pending invite. Resolution: verify "
                   "role/permissions, confirm seat assignment, resend invite, check plan entitlements."},
    "data_sync": {"title": "Data sync / integration issues",
        "keywords": ["sync", "integration", "connect", "api", "webhook", "not updating", "missing data", "stale", "import", "export"],
        "content": "Common causes: expired token, disconnected integration, rate limit. Resolution: "
                   "reconnect integration, refresh credentials, verify field mapping, check sync logs."},
}

KNOWLEDGE = {"incident": RUNBOOKS, "support": SUPPORT_KB}

MODE_FRAMING = {
    "incident": {
        "triage_role": "incident triage agent",
        "input_desc": "a dashboard, error screenshot, chart, or photo of a system problem",
        "kb_word": "runbooks",
    },
    "support": {
        "triage_role": "customer support triage agent",
        "input_desc": "a screenshot a customer sent of an error, bug, or confusing screen",
        "kb_word": "help-center articles",
    },
}

# ---------------------------------------------------------------------------
# Core model calls
# ---------------------------------------------------------------------------
def call_gemma(messages, schema, name, max_tokens=700, retries=1):
    last_err = None
    for _ in range(retries + 1):
        try:
            resp = client.chat.completions.create(
                model=MODEL, messages=messages, max_tokens=max_tokens,
                response_format={"type": "json_schema",
                                 "json_schema": {"name": name, "schema": schema, "strict": True}},
            )
            timing = getattr(resp, "time_info", {}) or {}
            return json.loads(resp.choices[0].message.content), timing
        except Exception as e:
            last_err = e
    raise last_err


VIDEO_EXTS = {'.mp4', '.mov', '.avi', '.webm', '.mkv'}

def _is_video(filename: str) -> bool:
    return os.path.splitext(filename.lower())[1] in VIDEO_EXTS

def extract_video_frames(raw: bytes, filename: str, n: int = 5) -> list:
    try:
        import cv2
    except ImportError:
        return []
    ext = os.path.splitext(filename.lower())[1] or '.mp4'
    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(suffix=ext, delete=False) as tmp:
            tmp.write(raw)
            tmp_path = tmp.name
        cap = cv2.VideoCapture(tmp_path)
        total = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        if total <= 0:
            cap.release()
            return []
        indices = [int(i * total / n) for i in range(n)]
        frames = []
        for idx in indices:
            cap.set(cv2.CAP_PROP_POS_FRAMES, idx)
            ret, frame = cap.read()
            if not ret:
                continue
            _, buf = cv2.imencode('.jpg', frame, [cv2.IMWRITE_JPEG_QUALITY, 85])
            b64 = base64.standard_b64encode(buf.tobytes()).decode()
            frames.append(f"data:image/jpeg;base64,{b64}")
        cap.release()
        return frames
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)


def triage_agent(image_uri, log_text="", mode="incident", image_uri2=None, frames=None):
    f = MODE_FRAMING[mode]
    dual = image_uri2 is not None and not frames
    if mode == "support":
        system = (f"You are a {f['triage_role']}. Look at the image(s) ({f['input_desc']}) and classify "
                  "the customer's issue. Set incident_type to a short issue label, severity to ticket "
                  "PRIORITY (low|medium|high|critical), retrieval_query to help-center keywords.")
        prompt = "Triage this support ticket."
    else:
        extra = " Two images are provided — treat the first as current state and the second as reference/before." if dual else ""
        system = (f"You are an {f['triage_role']}. Look at the image(s) ({f['input_desc']}) and classify "
                  f"the incident.{extra} If a log excerpt is provided, use it with the image(s). "
                  "incident_type is a short label. retrieval_query is keywords for the runbook knowledge needed.")
        prompt = "Triage this incident."
    content = [{"type": "text", "text": prompt}]
    if log_text.strip():
        label = "Customer message" if mode == "support" else "Attached log excerpt"
        content.append({"type": "text", "text": f"{label}:\n{log_text.strip()[:4000]}"})
    if frames:
        content.append({"type": "text", "text": f"Video analysis — {len(frames)} frames extracted evenly from the video:"})
        for frame_uri in frames:
            content.append({"type": "image_url", "image_url": {"url": frame_uri}})
    else:
        content.append({"type": "image_url", "image_url": {"url": image_uri}})
        if image_uri2:
            content.append({"type": "text", "text": "Reference / before image:"})
            content.append({"type": "image_url", "image_url": {"url": image_uri2}})
    return call_gemma(
        [{"role": "system", "content": system}, {"role": "user", "content": content}],
        TRIAGE_SCHEMA, "triage",
    )


def retrieval_agent(query, top_k=2, mode="incident"):
    """Gemma-powered semantic retrieval — picks the best KB entries by reasoning, not keyword counting."""
    kb = {**KNOWLEDGE[mode], **load_custom_kb()}
    if not kb:
        return [], {}, "(no entries in knowledge base)"

    kb_listing = "\n".join(
        f"key={k} | title={rb['title']} | keywords={', '.join(rb['keywords'])}"
        for k, rb in kb.items()
    )
    system = (
        "You are a knowledge base retrieval agent. Given an incident query and a list of KB entries, "
        f"select the {top_k} most relevant entries by their exact key names. "
        "Only select entries that are genuinely relevant to resolving this specific incident. "
        "If none are relevant, return an empty list. "
        "Be precise — irrelevant entries waste the resolution agent's context."
    )
    user = f"Incident query: {query}\n\nAvailable KB entries:\n{kb_listing}"
    try:
        result, timing = call_gemma(
            [{"role": "system", "content": system}, {"role": "user", "content": user}],
            RETRIEVAL_SCHEMA, "retrieval", max_tokens=300,
        )
        selected = [k for k in result.get("selected_keys", []) if k in kb][:top_k]
        return [(k, kb[k]) for k in selected], timing, result.get("reasoning", "")
    except Exception:
        # Fallback to keyword scoring if Gemma retrieval fails
        q = (query or "").lower()
        scored = [(sum(1 for kw in rb["keywords"] if kw in q), k, rb) for k, rb in kb.items()]
        scored = [(s, k, rb) for s, k, rb in scored if s > 0]
        scored.sort(reverse=True)
        return [(k, rb) for _, k, rb in scored[:top_k]], {}, "(fallback: keyword scoring)"


def resolution_agent(triage_data, runbooks, image_uri=None, image_uri2=None,
                     diagnostics=None, log_text="", mode="incident", frames=None):
    f = MODE_FRAMING[mode]
    rb_text = "\n\n".join(f"[{k}] {rb['title']}: {rb['content']}" for k, rb in runbooks) \
        if runbooks else f"(no relevant {f['kb_word']} found)"
    diag_text = ("\n\nDiagnostic findings:\n" +
                 "\n".join(f"- Q: {d['question']}\n  A: {d['answer']}" for d in diagnostics)) \
        if diagnostics else ""
    log_section = ""
    if log_text.strip():
        label = "Customer message" if mode == "support" else "Log excerpt (identify ROOT CAUSE)"
        log_section = f"\n\n{label}:\n{log_text.strip()[:4000]}"

    image_note = " The original image is also attached — use it to verify and enrich your reasoning." \
        if image_uri else ""
    if mode == "support":
        system = ("You are a customer support resolution agent. Produce action_plan as resolution steps. "
                  "Write draft_reply: a friendly message to send directly to the customer. "
                  "If diagnostic findings are present, set diagnostic_questions to []. "
                  "If confidence is not high and no diagnostic findings yet and no customer message, "
                  "set diagnostic_questions to 2-3 questions about details VISIBLE IN THE IMAGE. "
                  f"Otherwise diagnostic_questions is []. rationale is one sentence.{image_note}")
    else:
        system = ("You are an incident resolution agent. Produce action_plan as short step strings. "
                  "Set draft_reply to an empty string. If log excerpt present, identify ROOT CAUSE in rationale. "
                  "If diagnostic findings present, use them and set diagnostic_questions to []. "
                  "If confidence is not high and no diagnostic findings and no log excerpt, "
                  "set diagnostic_questions to 2-3 questions about details VISIBLE IN THE IMAGE. "
                  f"Otherwise diagnostic_questions is [].{image_note}")
    user_text = (f"Triage:\n{json.dumps(triage_data, indent=2)}\n\n"
                 f"Retrieved {f['kb_word']}:\n{rb_text}{diag_text}{log_section}\n\nProduce the resolution.")
    if frames:
        user_content = [{"type": "text", "text": user_text}]
        for frame_uri in frames:
            user_content.append({"type": "image_url", "image_url": {"url": frame_uri}})
    elif image_uri:
        user_content = [{"type": "text", "text": user_text},
                        {"type": "image_url", "image_url": {"url": image_uri}}]
        if image_uri2:
            user_content.append({"type": "text", "text": "Reference / before image:"})
            user_content.append({"type": "image_url", "image_url": {"url": image_uri2}})
    else:
        user_content = user_text
    return call_gemma(
        [{"role": "system", "content": system}, {"role": "user", "content": user_content}],
        RESOLUTION_SCHEMA, "resolution",
    )


def diagnostic_agent(image_uri, questions, image_uri2=None, frames=None):
    dual = image_uri2 is not None and not frames
    system = ("You are a diagnostic agent. Re-examine the image(s) carefully and answer each question "
              "based only on what you can actually see. "
              + ("Compare the two images where relevant — the first is current state, the second is reference. " if dual else "")
              + "Be concise and factual.")
    q_list = "\n".join(f"- {q}" for q in questions)
    if frames:
        content = [{"type": "text", "text": f"Answer these about the image(s):\n{q_list}"}]
        for frame_uri in frames:
            content.append({"type": "image_url", "image_url": {"url": frame_uri}})
    else:
        content = [
            {"type": "text", "text": f"Answer these about the image(s):\n{q_list}"},
            {"type": "image_url", "image_url": {"url": image_uri}},
        ]
        if image_uri2:
            content.append({"type": "text", "text": "Reference / before image:"})
            content.append({"type": "image_url", "image_url": {"url": image_uri2}})
    data, timing = call_gemma(
        [{"role": "system", "content": system}, {"role": "user", "content": content}],
        DIAGNOSTIC_SCHEMA, "diagnostic",
    )
    return data.get("answers", []), timing


# ---------------------------------------------------------------------------
# SSE helper
# ---------------------------------------------------------------------------
def sse(event, data):
    return f"event: {event}\ndata: {json.dumps(data)}\n\n"


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------
def run_pipeline(image_uri, log_text="", mode="incident", filename="", image_uri2=None, frames=None):
    try:
        yield from _pipeline(image_uri, log_text, mode, filename, image_uri2, frames=frames)
    except Exception as e:
        yield sse("error", {"message": "An agent call failed. Please try again.", "detail": str(e)[:200]})


def _pipeline(image_uri, log_text="", mode="incident", filename="", image_uri2=None, frames=None):
    total = 0.0
    kb_word = MODE_FRAMING[mode]["kb_word"]
    dual = image_uri2 is not None and not frames

    # Start GPU comparison in background immediately — runs in parallel with the whole pipeline
    gpu_provider = _get_gpu_provider()
    gpu_result_q = qmod.Queue()
    if gpu_provider:
        gpu_key, gpu_base_url, gpu_model, gpu_label = gpu_provider
        gpu_t0 = time.perf_counter()
        def _gpu_parallel():
            # Run 4 sequential calls mirroring the Cerebras pipeline so times are comparable
            _calls = [
                ("You are an incident triage agent.",
                 "Analyze this incident: multiple services reporting connection errors, DB pool at 98%, "
                 "error rate spiked 0.1%->12%. Classify with incident_type, severity, summary, retrieval_query. Respond with JSON.",
                 200),
                ("You are a knowledge retrieval agent.",
                 "Given incident: DB connection pool exhausted, high error rate. "
                 "Select the most relevant runbooks from: database_connection, high_latency, disk_full, error_rate_spike, cpu_saturation. "
                 "Respond with JSON: {selected_keys: [...], reasoning: '...'}",
                 150),
                ("You are an incident resolution agent.",
                 "DB connection pool exhausted (98%). Runbook: check pool max size, find leaks, scale replicas. "
                 "Produce action plan with confidence (low/medium/high) and rationale. Respond with JSON.",
                 300),
                ("You are a diagnostic agent.",
                 "Re-examine the incident: DB pool at 98%, error rate 12%. "
                 "Answer: 1) Are there signs of connection leaks? 2) What is the recommended immediate action? Respond with JSON.",
                 200),
            ]
            try:
                gc = OpenAI(api_key=gpu_key, base_url=gpu_base_url)
                for sys_msg, user_msg, max_tok in _calls:
                    gc.chat.completions.create(
                        model=gpu_model,
                        messages=[{"role": "system", "content": sys_msg},
                                  {"role": "user", "content": user_msg}],
                        max_tokens=max_tok,
                    )
                gpu_result_q.put(("g", round((time.perf_counter() - gpu_t0) * 1000, 1), gpu_label, False))
            except Exception as e:
                print(f"[pipeline gpu error] {type(e).__name__}: {e}")
                gpu_result_q.put(("g_err", round((time.perf_counter() - gpu_t0) * 1000, 1), f"{gpu_label} (error: {type(e).__name__})", True))
        Thread(target=_gpu_parallel, daemon=True).start()

    # --- TRIAGE AGENT ---
    label = "Triage agent analyzing images" if dual else ("Triage agent analyzing video frames" if frames else "Triage agent analyzing image")
    yield sse("status", {"stage": "triage", "label": label})
    triage, t1 = triage_agent(image_uri, log_text, mode=mode, image_uri2=image_uri2, frames=frames)
    total += t1.get("total_time", 0)
    yield sse("triage", {"data": triage, "ms": round(t1.get("total_time", 0) * 1000, 1),
                         "total_ms": round(total * 1000, 1), "has_log": bool(log_text.strip()),
                         "mode": mode, "dual": dual, "video": bool(frames), "frame_count": len(frames) if frames else 0})
    yield sse("wire", {
        "from": "triage", "to": "retrieval", "label": "retrieval_query",
        "payload": triage.get("retrieval_query", ""),
        "note": f"incident_type={triage.get('incident_type')}  severity={triage.get('severity')}",
    })

    # --- RETRIEVAL AGENT (Gemma semantic) ---
    yield sse("status", {"stage": "retrieval", "label": f"Retrieval agent — Gemma semantic search over {kb_word}"})
    runbooks, t2, reasoning = retrieval_agent(triage.get("retrieval_query", ""), mode=mode)
    total += t2.get("total_time", 0)
    rb_list = [{"key": k, "title": rb["title"]} for k, rb in runbooks]
    yield sse("retrieval", {"runbooks": rb_list, "ms": round(t2.get("total_time", 0) * 1000, 1),
                            "total_ms": round(total * 1000, 1), "reasoning": reasoning})
    yield sse("wire", {
        "from": "retrieval", "to": "resolution",
        "label": f"selected {len(runbooks)} {kb_word}",
        "payload": [rb["title"] for _, rb in runbooks] or ["(none relevant)"],
        "note": reasoning or "Gemma semantic selection",
    })

    # --- RESOLUTION AGENT pass 1 (now sees image) ---
    yield sse("status", {"stage": "resolution", "label": "Resolution agent synthesizing (with image context)"})
    resolution, t3 = resolution_agent(triage, runbooks, image_uri=image_uri, image_uri2=image_uri2,
                                      log_text=log_text, mode=mode, frames=frames)
    total += t3.get("total_time", 0)
    conf = resolution.get("confidence", "medium")
    yield sse("resolution", {"data": resolution, "ms": round(t3.get("total_time", 0) * 1000, 1),
                             "total_ms": round(total * 1000, 1), "pass": 1})

    rose = False
    if CONF_RANK.get(conf, 1) < CONF_RANK["high"]:
        questions = resolution.get("diagnostic_questions") or []
        if questions:
            yield sse("wire", {
                "from": "resolution", "to": "diagnostic",
                "label": f"confidence={conf} — delegating {len(questions)} questions",
                "payload": questions,
                "note": "resolution loop: asking diagnostic agent for more visual evidence",
            })
            yield sse("status", {"stage": "diagnostic", "label": "Diagnostic agent re-examining image(s)"})
            yield sse("loop_questions", {"questions": questions})
            answers, t4 = diagnostic_agent(image_uri, questions, image_uri2=image_uri2, frames=frames)
            total += t4.get("total_time", 0)
            yield sse("diagnostic", {"answers": answers, "ms": round(t4.get("total_time", 0) * 1000, 1),
                                     "total_ms": round(total * 1000, 1)})
            yield sse("wire", {
                "from": "diagnostic", "to": "resolution",
                "label": f"answered {len(answers)} questions",
                "payload": [f"Q: {a['question']}  |  A: {a['answer']}" for a in answers],
                "note": "re-synthesizing with new visual evidence",
            })

            yield sse("status", {"stage": "resolution2", "label": "Resolution re-synthesizing"})
            resolution2, t3b = resolution_agent(triage, runbooks, image_uri=image_uri, image_uri2=image_uri2,
                                                diagnostics=answers, log_text=log_text, mode=mode, frames=frames)
            total += t3b.get("total_time", 0)
            new_conf = resolution2.get("confidence", conf)
            rose = CONF_RANK.get(new_conf, 1) > CONF_RANK.get(conf, 1)
            resolution = resolution2
            yield sse("resolution", {"data": resolution, "ms": round(t3b.get("total_time", 0) * 1000, 1),
                                     "total_ms": round(total * 1000, 1), "pass": 2,
                                     "old_conf": conf, "new_conf": new_conf, "rose": rose})

    total_ms = round(total * 1000, 1)

    # Persist to audit log
    try:
        save_triage_log(mode, triage, resolution, total_ms, filename)
    except Exception:
        pass

    # Webhook
    webhook_url = get_setting("webhook_url", "")
    if webhook_url:
        payload = {"ts": time.time(), "mode": mode, "triage": triage,
                   "resolution": resolution, "total_ms": total_ms, "filename": filename}
        try:
            post_webhook(webhook_url, payload)
            yield sse("webhook", {"ok": True, "url": webhook_url})
        except Exception as e:
            yield sse("webhook", {"ok": False, "url": webhook_url, "error": str(e)[:120]})

    yield sse("done", {"total_ms": total_ms, "looped": rose, "rose": rose,
                       "result": {"triage": triage, "resolution": resolution}})

    # Emit GPU comparison result (was running in parallel the whole time)
    if gpu_provider:
        try:
            item = gpu_result_q.get(timeout=60)
            is_error = item[0] == "g_err"
            yield sse("bench_gpu", {"ms": item[1], "label": item[2], "estimated": False, "error": is_error})
        except qmod.Empty:
            pass


# ---------------------------------------------------------------------------
# Benchmark
# ---------------------------------------------------------------------------
BENCHMARK_PROMPT = (
    "Multiple services are reporting connection errors. The database connection pool is at 98% capacity. "
    "Error rate has spiked from 0.1% to 12% in the last 5 minutes. CPU is normal. "
    "Classify this incident: set incident_type to a short label, severity, one-sentence summary, "
    "and retrieval_query keywords for the relevant runbook."
)


def _get_gpu_provider():
    """Return (api_key, base_url, model, label) for whichever GPU provider is configured."""
    if os.environ.get("GROQ_API_KEY"):
        return (os.environ["GROQ_API_KEY"],
                "https://api.groq.com/openai/v1",
                "llama-3.3-70b-versatile",
                "Groq · Llama-3.3-70B (GPU)")
    if os.environ.get("TOGETHER_API_KEY"):
        return (os.environ["TOGETHER_API_KEY"],
                "https://api.together.xyz/v1",
                "google/gemma-2-27b-it",
                "Together AI · Gemma-2-27B (GPU)")
    if os.environ.get("FIREWORKS_API_KEY"):
        return (os.environ["FIREWORKS_API_KEY"],
                "https://api.fireworks.ai/inference/v1",
                "accounts/fireworks/models/llama-v3p1-8b-instruct",
                "Fireworks AI · Llama-3.1-8B (GPU)")
    return None


def _benchmark_stream():
    gpu_provider = _get_gpu_provider()

    if not gpu_provider:
        # Cerebras only — no GPU estimate, no fake numbers
        yield sse("bench_status", {"msg": "Running Cerebras benchmark..."})
        t0 = time.perf_counter()
        try:
            resp = client.chat.completions.create(
                model=MODEL,
                messages=[{"role": "system", "content": "You are an incident triage agent."},
                          {"role": "user", "content": BENCHMARK_PROMPT}],
                max_tokens=200,
                response_format={"type": "json_schema",
                                 "json_schema": {"name": "triage_bench", "schema": TRIAGE_SCHEMA, "strict": True}},
            )
            cerebras_ms = round((time.perf_counter() - t0) * 1000, 1)
            tokens = (resp.usage.completion_tokens or 80) if resp.usage else 80
        except Exception:
            cerebras_ms = round((time.perf_counter() - t0) * 1000, 1)
            tokens = 80

        yield sse("bench_cerebras", {"ms": cerebras_ms, "tokens": tokens})
        yield sse("bench_done", {"cerebras_ms": cerebras_ms, "gpu_ms": None})

    else:
        gpu_key, gpu_base_url, gpu_model, gpu_label = gpu_provider

        # Parallel: both providers via threads
        result_q = qmod.Queue()

        def cerebras_worker():
            t0 = time.perf_counter()
            try:
                resp = client.chat.completions.create(
                    model=MODEL,
                    messages=[{"role": "system", "content": "You are an incident triage agent."},
                              {"role": "user", "content": BENCHMARK_PROMPT}],
                    max_tokens=200,
                    response_format={"type": "json_schema",
                                     "json_schema": {"name": "triage_bench", "schema": TRIAGE_SCHEMA, "strict": True}},
                )
                ms = round((time.perf_counter() - t0) * 1000, 1)
                tok = (resp.usage.completion_tokens or 80) if resp.usage else 80
                result_q.put(("c", ms, tok))
            except Exception:
                result_q.put(("c", round((time.perf_counter() - t0) * 1000, 1), 80))

        def gpu_worker():
            t0 = time.perf_counter()
            _calls = [
                ("You are an incident triage agent.",
                 "Analyze this incident: multiple services reporting connection errors, DB pool at 98%, "
                 "error rate spiked 0.1%->12%. Classify with incident_type, severity, summary, retrieval_query. Respond with JSON.",
                 200),
                ("You are a knowledge retrieval agent.",
                 "Given incident: DB connection pool exhausted, high error rate. "
                 "Select the most relevant runbooks from: database_connection, high_latency, disk_full, error_rate_spike, cpu_saturation. "
                 "Respond with JSON: {selected_keys: [...], reasoning: '...'}",
                 150),
                ("You are an incident resolution agent.",
                 "DB connection pool exhausted (98%). Runbook: check pool max size, find leaks, scale replicas. "
                 "Produce action plan with confidence (low/medium/high) and rationale. Respond with JSON.",
                 300),
                ("You are a diagnostic agent.",
                 "Re-examine the incident: DB pool at 98%, error rate 12%. "
                 "Answer: 1) Are there signs of connection leaks? 2) What is the recommended immediate action? Respond with JSON.",
                 200),
            ]
            try:
                gc = OpenAI(api_key=gpu_key, base_url=gpu_base_url)
                for sys_msg, user_msg, max_tok in _calls:
                    gc.chat.completions.create(
                        model=gpu_model,
                        messages=[{"role": "system", "content": sys_msg},
                                  {"role": "user", "content": user_msg}],
                        max_tokens=max_tok,
                    )
                result_q.put(("g", round((time.perf_counter() - t0) * 1000, 1), gpu_label, False))
            except Exception as e:
                print(f"[benchmark gpu_worker error] {type(e).__name__}: {e}")
                result_q.put(("g_err", round((time.perf_counter() - t0) * 1000, 1), f"{gpu_label} (error: {type(e).__name__})", True))

        Thread(target=cerebras_worker, daemon=True).start()
        Thread(target=gpu_worker, daemon=True).start()

        cerebras_ms = None
        gpu_ms = None
        remaining = 2

        while remaining > 0:
            try:
                item = result_q.get(timeout=60)
            except qmod.Empty:
                break
            remaining -= 1
            if item[0] == "c":
                cerebras_ms = item[1]
                yield sse("bench_cerebras", {"ms": cerebras_ms, "tokens": item[2]})
            else:
                gpu_ms = item[1]
                is_error = item[0] == "g_err"
                yield sse("bench_gpu", {"ms": gpu_ms, "label": item[2], "estimated": item[3], "error": is_error})

        if cerebras_ms and gpu_ms:
            speedup = round(gpu_ms / cerebras_ms, 1) if cerebras_ms > 0 else 1.0
            yield sse("bench_done", {"speedup": speedup, "cerebras_ms": cerebras_ms, "gpu_ms": gpu_ms})


# ---------------------------------------------------------------------------
# Auth dependency (optional API key — only enforced when TRIAGEFLOW_API_KEY is set)
# ---------------------------------------------------------------------------
def require_api_key(x_api_key: Optional[str] = Header(default=None)):
    expected = os.environ.get("TRIAGEFLOW_API_KEY")
    if expected and x_api_key != expected:
        raise HTTPException(status_code=401,
                            detail={"error": "Invalid or missing API key", "hint": "Set X-API-Key header"})


# ---------------------------------------------------------------------------
# FastAPI
# ---------------------------------------------------------------------------
app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# Serve demo images as static files
_demo_dir = os.path.join(os.path.dirname(__file__), "demo_images")
if os.path.isdir(_demo_dir):
    app.mount("/demo-images", StaticFiles(directory=_demo_dir), name="demo-images")

DEMO_SCENARIOS = [
    {"id": "error_spike", "title": "API Error Spike + Deploy", "desc": "5xx spike correlated with a payments-service deploy",
     "img1": "01_error_spike.jpg", "img2": "05_deploy_latency.jpg", "log": ""},
    {"id": "disk_full", "title": "Disk Space Exhausted", "desc": "Root filesystem at 100%, errno 28 write failures",
     "img1": "02_disk_full.jpg", "img2": None, "log": ""},
    {"id": "db_pool", "title": "DB Connection Pool", "desc": "orders-db-primary at max connections",
     "img1": "03_db_pool.jpg", "img2": None, "log": ""},
    {"id": "deploy_latency", "title": "Deploy-induced Latency", "desc": "P99 spike immediately after rollout v2.4.1",
     "img1": "05_deploy_latency.jpg", "img2": "Grafana.jpg", "log": ""},
]

@app.get("/demo-scenarios")
async def demo_scenarios_get():
    valid = []
    for s in DEMO_SCENARIOS:
        if os.path.isdir(_demo_dir) and os.path.exists(os.path.join(_demo_dir, s["img1"])):
            valid.append({**s, "img1": f"/demo-images/{s['img1']}",
                          "img2": f"/demo-images/{s['img2']}" if s["img2"] else None})
    return JSONResponse(valid)


def _encode_upload(raw: bytes, filename: str) -> str:
    ext = (filename or "x.jpg").split(".")[-1].lower()
    media = "jpeg" if ext in ("jpg", "jpeg") else ext
    return f"data:image/{media};base64,{base64.standard_b64encode(raw).decode()}"


@app.post("/triage")
async def triage_endpoint(
    file: UploadFile = File(...),
    file2: UploadFile = File(None),
    log: str = Form(""),
    mode: str = Form("incident"),
    _auth=Depends(require_api_key),
):
    if mode not in KNOWLEDGE:
        mode = "incident"
    raw = await file.read()
    fname = file.filename or ""
    frames = None
    if _is_video(fname):
        frames = extract_video_frames(raw, fname, n=5)
        image_uri = frames[0] if frames else _encode_upload(raw[:1024], "placeholder.jpg")
        image_uri2 = None
    else:
        image_uri = _encode_upload(raw, fname)
        image_uri2 = None
        if file2 and file2.filename:
            raw2 = await file2.read()
            image_uri2 = _encode_upload(raw2, file2.filename)
    return StreamingResponse(
        run_pipeline(image_uri, log, mode, fname, image_uri2=image_uri2, frames=frames),
        media_type="text/event-stream",
    )


@app.post("/benchmark")
async def benchmark(_auth=Depends(require_api_key)):
    return StreamingResponse(_benchmark_stream(), media_type="text/event-stream")


# History
@app.get("/history")
async def history_get():
    with get_db() as con:
        rows = con.execute(
            "SELECT id,ts,mode,incident_type,severity,confidence,action_plan,rationale,total_ms,filename "
            "FROM triage_log ORDER BY ts DESC LIMIT 20"
        ).fetchall()
    return JSONResponse([{
        "id": r["id"], "ts": r["ts"], "mode": r["mode"],
        "incident_type": r["incident_type"], "severity": r["severity"],
        "confidence": r["confidence"],
        "action_plan": json.loads(r["action_plan"] or "[]"),
        "rationale": r["rationale"], "total_ms": r["total_ms"], "filename": r["filename"],
    } for r in rows])


@app.delete("/history")
async def history_clear():
    with get_db() as con:
        con.execute("DELETE FROM triage_log")
    return JSONResponse({"ok": True})


# Custom KB
@app.get("/kb")
async def kb_get():
    with get_db() as con:
        rows = con.execute("SELECT key,title,keywords,content,created_at FROM custom_kb ORDER BY created_at DESC").fetchall()
    return JSONResponse([{
        "key": r["key"], "title": r["title"],
        "keywords": json.loads(r["keywords"]),
        "content": r["content"], "created_at": r["created_at"],
    } for r in rows])


class KBEntry(BaseModel):
    key: str = ""
    title: str
    keywords: list[str]
    content: str


@app.post("/kb")
async def kb_add(entry: KBEntry):
    key = entry.key or re.sub(r"[^a-z0-9]+", "_", entry.title.lower()).strip("_")
    with get_db() as con:
        con.execute(
            "INSERT OR REPLACE INTO custom_kb (key,title,keywords,content,created_at) VALUES (?,?,?,?,?)",
            (key, entry.title, json.dumps(entry.keywords), entry.content, time.time()),
        )
    return JSONResponse({"key": key, "ok": True})


@app.delete("/kb/{key}")
async def kb_delete(key: str):
    with get_db() as con:
        con.execute("DELETE FROM custom_kb WHERE key=?", (key,))
    return JSONResponse({"ok": True})


# Settings
@app.get("/settings")
async def settings_get():
    gpu = _get_gpu_provider()
    return JSONResponse({
        "webhook_url": get_setting("webhook_url", ""),
        "has_together_key": gpu is not None,
        "gpu_label": gpu[3] if gpu else None,
    })


class SettingsUpdate(BaseModel):
    webhook_url: str = ""


@app.post("/settings")
async def settings_update(s: SettingsUpdate):
    set_setting("webhook_url", s.webhook_url.strip())
    return JSONResponse({"ok": True})


@app.post("/settings/test-webhook")
async def test_webhook(s: SettingsUpdate):
    url = s.webhook_url.strip()
    if not url:
        return JSONResponse({"ok": False, "error": "No URL provided"})
    try:
        post_webhook(url, {"test": True, "source": "TriageFlow", "ts": time.time()})
        return JSONResponse({"ok": True})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)[:200]})


@app.get("/health")
async def health():
    db_status = "ok"
    try:
        with get_db() as con:
            con.execute("SELECT 1")
    except Exception:
        db_status = "error"
    gpu = _get_gpu_provider()
    return JSONResponse({
        "status": "ok",
        "model": MODEL,
        "version": "1.0.0",
        "db": db_status,
        "gpu_provider": gpu[3] if gpu else None,
    })


@app.get("/")
async def index():
    with open(os.path.join(os.path.dirname(__file__), "index.html"), encoding="utf-8") as f:
        return HTMLResponse(f.read())
