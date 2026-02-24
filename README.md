# Apollo Enrichment Pipeline

A production-style enrichment pipeline with **FastAPI backend** and **Next.js frontend**. Upload Excel files, enrich via Apollo.io APIs, scrape websites with an AI extractor, and store results in SQLite (local) or PostgreSQL/Supabase (production).

## Features

- **Upload & Enrich**: Upload .xlsx files; enrich people and organizations via Apollo and store in the database
- **Scrape & Enrich**: Enter any URL; AI extractor (Apify) extracts names and organizations, then enriches and saves
- **Upload Base Data**: Import Excel directly without Apollo enrichment
- **Database Viewer**: Search, filter, paginate, and export records to Excel
- **Database**: SQLite locally; PostgreSQL (Supabase/Neon) for production
- **PII protection**: Automatic masking in logs; idempotent upserts by email

## Architecture

```
apollo-db/
├── backend/           # FastAPI API
│   ├── main.py        # Routes (enrich, upload, db stats/records/export)
│   └── services.py    # Pipeline core (no UI)
├── frontend/          # Next.js (App Router, Tailwind)
│   └── src/app/       # Pages: upload-enrich, scrape-enrich, upload-base, database
├── config.py          # Configuration (env)
├── db.py              # Database layer (SQLite + PostgreSQL)
├── apollo.py          # Apollo API client
├── ingest.py          # Excel ingestion & enrichment
├── scraper.py         # Apify AI extractor
├── utils.py           # Utilities (PII masking, validation)
├── requirements.txt   # Python dependencies
└── .env               # Environment variables (not committed)
```

## Prerequisites

- **Python 3.10+** (backend)
- **Node.js 18+** (frontend)
- **Apollo.io API key** ([Get one](https://app.apollo.io/))
- Optional: **Apify token** for Scrape & Enrich ([Apify Console](https://console.apify.com/settings/integrations))

## Installation

### 1. Backend

```bash
python3 -m venv venv
source venv/bin/activate   # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

Copy environment and set required keys:

```bash
cp .env.example .env
# Edit .env: APOLLO_API_KEY=... (and optionally APIFY_API_TOKEN, DATABASE_URL)
```

### 2. Frontend

```bash
cd frontend
npm install
```

Create `frontend/.env.local` (optional; defaults to `http://localhost:8000`):

```
NEXT_PUBLIC_API_URL=http://localhost:8000
```

## Usage

### Run locally

1. **Start the backend** (from project root):

   ```bash
   uvicorn backend.main:app --reload
   ```

   API runs at `http://localhost:8000`. Docs: `http://localhost:8000/docs`.

2. **Start the frontend**:

   ```bash
   cd frontend && npm run dev
   ```

   App runs at `http://localhost:3000`.

### API endpoints

- `POST /enrich/upload` — Upload Excel and run enrichment (multipart: file, enrich_people, enrich_companies)
- `POST /enrich/scrape` — Scrape URL and enrich (JSON: url, enrich_people, enrich_companies)
- `POST /upload/base` — Upload Excel without enrichment
- `GET /db/stats` — Database statistics
- `GET /db/records` — Search records (query params: filters, limit, offset)
- `GET /db/columns` — Base and Apollo column lists
- `GET /db/export` — Export filtered data as Excel (same query params as search)

### Environment variables

- **Backend** (`.env` in project root):
  - `APOLLO_API_KEY` — **Required** for enrichment
  - `APIFY_API_TOKEN` — Required for Scrape & Enrich
  - `DATABASE_URL` — PostgreSQL URI (e.g. Supabase); omit for SQLite
  - `DB_PATH` — SQLite path when `DATABASE_URL` is unset (default: apollo_truth.db)
  - `CORS_ORIGINS` — Comma-separated origins (default: http://localhost:3000)
  - See `.env.example` and `config.py` for full list.

- **Frontend** (`frontend/.env.local`):
  - `NEXT_PUBLIC_API_URL` — Backend base URL (default: http://localhost:8000)

## Production / deployment

- **Backend**: Deploy to Railway, Render, Fly.io, or Cloud Run. Run `uvicorn backend.main:app --host 0.0.0.0 --port $PORT`. Set `DATABASE_URL`, `APOLLO_API_KEY`, `APIFY_API_TOKEN`, and `CORS_ORIGINS` (e.g. your frontend URL).
- **Frontend**: Deploy to Vercel or Netlify. Set `NEXT_PUBLIC_API_URL` to your backend URL.
- **Database**: Use Supabase or Neon; set `DATABASE_URL` in the backend only.

## Database schema

The **Truth** table uses **Email ID (unique)** as the unique key. Base columns (e.g. First Name, Last Name, Company, Website, Lead Source) are fixed; Apollo enrichment adds columns prefixed with `Apollo Person:` or `Apollo Company:`.

## Development

### Tests

```bash
pytest tests/ -v
pytest tests/ --cov=. --cov-report=term-missing
```

### Running the old entrypoint

`python app.py` prints instructions to run the FastAPI backend and Next.js frontend; the previous Streamlit UI has been removed.

## License

Proprietary. All rights reserved.
