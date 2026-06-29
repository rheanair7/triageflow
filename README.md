# TriageFlow

Multimodal multi-agent incident triage powered by **Gemma 4 31B** on the **Cerebras Wafer-Scale Engine**.

Upload a dashboard screenshot, Grafana chart, before/after image pair, or an MP4 video — four coordinated agents classify the incident, retrieve relevant runbooks, produce a ranked action plan, and run a confidence loop that fires a diagnostic re-examination when needed. Full 4-agent pipeline completes in **~400ms**.

Built for the [Cerebras + Google DeepMind Gemma 4 Hackathon](https://cerebras.ai) — Track 1 (Multiverse Agents) and Track 3 (Enterprise Impact).

---

## Demo

Open http://localhost:8000 to see the live UI with the SVG architecture diagram.

**10.4× faster** than GPU on the same 4-agent workload (Cerebras Gemma 4 31B vs Groq Llama 3.3-70B, measured simultaneously).

---

## Agent Pipeline

```
Upload (image / video / dual image)
    │
    ▼
Triage Agent ──────── vision · classify incident type + severity
    │
    ▼
Retrieval Agent ───── Gemma semantic · select relevant runbooks
    │
    ▼
Resolution Agent ──── vision + text · action plan + confidence score
    │
    ├─── confidence = HIGH ──► Done
    │
    └─── confidence < HIGH
              │
              ▼
         Diagnostic Agent ── vision · re-examine image, answer questions
              │
              ▼
         Resolution Agent (pass 2) ── re-synthesize with findings
```

All four agents run on **Gemma 4 31B** via the Cerebras API. Vision agents receive the image alongside text context — the model sees what the SRE sees.

---

## Features

- **Multimodal inputs** — single image, dual image (before/after), or video (5 frames extracted)
- **Gemma semantic retrieval** — retrieval agent reasons over the KB rather than keyword matching
- **Confidence loop** — resolution agent requests diagnostic re-examination when uncertain
- **Agent Wire** — live SSE stream showing inter-agent message passing in real time
- **Speed benchmark** — GPU comparison runs in parallel with every triage, auto-populates result
- **Custom knowledge base** — add/remove runbooks via UI, persisted in SQLite
- **Triage history** — full audit log of every run
- **Webhook / Slack integration** — fires on every triage completion
- **Incident report download** — `.txt` artifact with action plan and root cause
- **Docker deployment** — one-command deploy with `docker-compose up`
- **API key auth** — optional `X-API-Key` middleware
- **Health check** — `GET /health` with model, DB, and GPU provider status

---

## Quickstart

### Local

```bash
git clone https://github.com/your-username/triageflow.git
cd triageflow

python -m venv venv
venv\Scripts\activate        # Windows
# source venv/bin/activate   # Mac/Linux

pip install -r requirements.txt
pip install opencv-python    # for video support

set CEREBRAS_API_KEY=your_key_here
set GROQ_API_KEY=your_groq_key_here   # optional — enables GPU benchmark

uvicorn backend:app --port 8000
```

Open http://localhost:8000

### Docker

```bash
cp .env.example .env
# fill in CEREBRAS_API_KEY and optionally GROQ_API_KEY

docker-compose up --build
```

Open http://localhost:8000

---

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `CEREBRAS_API_KEY` | Yes | Cerebras API key — get one at [cerebras.ai](https://cerebras.ai) |
| `GROQ_API_KEY` | No | Enables live GPU benchmark (free at [console.groq.com](https://console.groq.com)) |
| `FIREWORKS_API_KEY` | No | Alternative GPU provider for benchmark |
| `TOGETHER_API_KEY` | No | Alternative GPU provider for benchmark |
| `TRIAGEFLOW_API_KEY` | No | Enables API key auth on `/triage` and `/benchmark` |
| `DATA_DIR` | No | SQLite storage path (defaults to `triageflow_app/`, set to `/app/data` in Docker) |

---

## API

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/` | UI |
| `POST` | `/triage` | Run pipeline (SSE stream). Fields: `file`, `file2` (optional), `log` (optional), `mode` (incident/support) |
| `POST` | `/benchmark` | Run standalone GPU comparison (SSE stream) |
| `GET` | `/health` | Model, DB, and GPU provider status |
| `GET` | `/history` | Last 20 triage runs |
| `DELETE` | `/history` | Clear audit log |
| `GET` | `/kb` | List custom KB entries |
| `POST` | `/kb` | Add KB entry |
| `DELETE` | `/kb/{key}` | Remove KB entry |
| `GET` | `/settings` | Webhook URL and GPU provider config |
| `POST` | `/settings` | Update webhook URL |
| `GET` | `/demo-scenarios` | List available demo scenarios |

---

## Stack

- **Model** — Gemma 4 31B (`gemma-4-31b`) via Cerebras API
- **Backend** — FastAPI + Server-Sent Events
- **Storage** — SQLite (audit log, custom KB, settings)
- **Video** — OpenCV frame extraction
- **GPU benchmark** — OpenAI-compatible clients (Groq / Fireworks / Together AI)
- **Frontend** — single-file HTML/CSS/JS, no build step

---

## Project Structure

```
triageflow/
├── backend.py           # FastAPI app, all 4 agents, SSE pipeline
├── index.html           # Single-file UI
├── requirements.txt
├── Dockerfile
├── docker-compose.yml
├── demo_images/         # Test images for demo mode
├── .env.example
└── README.md
```
