"""
FastAPI application for Apollo enrichment pipeline.
Run from project root: uvicorn backend.main:app --reload
"""

import os
from contextlib import asynccontextmanager
from typing import Any, Optional

from fastapi import Depends, FastAPI, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from pydantic import BaseModel

from config import config, load_config
from db import DatabaseManager

from . import services


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Create a single shared database connection at startup; close on shutdown."""
    load_config(validate=False)
    db = DatabaseManager()
    app.state.db = db
    yield
    db.close()


def get_db(request: Request) -> DatabaseManager:
    """FastAPI dependency: return the shared database instance (do not close)."""
    return request.app.state.db


app = FastAPI(
    title="Apollo Enrichment API",
    description="REST API for upload, enrich, scrape, and database operations",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS: allow frontend origin from env or default for dev
CORS_ORIGINS = os.getenv("CORS_ORIGINS", "http://localhost:3000").strip().split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[o.strip() for o in CORS_ORIGINS if o.strip()],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Query param name -> Truth table column name (for search/export)
FILTER_PARAM_TO_COLUMN = {
    "email": "Email ID (unique)",
    "company": "Company Name (Based on Website Domain)",
    "country": "Country",
    "first_name": "First Name",
    "last_name": "Last Name",
    "job_title": "Job Title",
    "industry": "Industry",
    "state": "State",
    "website": "Website URLs",
    "lead_source": "Lead Source",
    "client_type": "Client Type",
    "email_send": "Email Send (Yes/No)",
}


def _build_filters(
    email: Optional[str] = None,
    company: Optional[str] = None,
    country: Optional[str] = None,
    first_name: Optional[str] = None,
    last_name: Optional[str] = None,
    job_title: Optional[str] = None,
    industry: Optional[str] = None,
    state: Optional[str] = None,
    website: Optional[str] = None,
    lead_source: Optional[str] = None,
    client_type: Optional[str] = None,
    email_send: Optional[str] = None,
) -> dict[str, Any]:
    """Build filters dict from query params (empty strings omitted)."""
    filters: dict[str, Any] = {}
    params = {
        "email": email,
        "company": company,
        "country": country,
        "first_name": first_name,
        "last_name": last_name,
        "job_title": job_title,
        "industry": industry,
        "state": state,
        "website": website,
        "lead_source": lead_source,
        "client_type": client_type,
        "email_send": email_send,
    }
    for param, value in params.items():
        if value is not None and str(value).strip():
            col = FILTER_PARAM_TO_COLUMN[param]
            filters[col] = value.strip()
    return filters


# ---------------------------------------------------------------------------
# Enrichment routes
# ---------------------------------------------------------------------------


@app.post("/enrich/upload")
async def enrich_upload(
    file: UploadFile = File(...),
    enrich_people: bool = Form(True),
    enrich_companies: bool = Form(True),
    db: DatabaseManager = Depends(get_db),
):
    """Upload Excel file and run enrichment pipeline."""
    if not config.APOLLO_API_KEY:
        raise HTTPException(
            status_code=503,
            detail="APOLLO_API_KEY is not set. Set it before using enrichment.",
        )
    if not file.filename or not file.filename.lower().endswith(".xlsx"):
        raise HTTPException(status_code=400, detail="File must be an .xlsx Excel file.")
    try:
        file_bytes = await file.read()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to read file: {e}") from e
    try:
        load_config(validate=True)
        results = services.run_enrichment_pipeline_core(
            file_bytes,
            enrich_people=enrich_people,
            enrich_companies=enrich_companies,
            db=db,
        )
        if results is None:
            raise HTTPException(status_code=500, detail="Enrichment pipeline returned no results.")
        return results
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


class ScrapeBody(BaseModel):
    """Request body for scrape & enrich."""

    url: str
    enrich_people: bool = True
    enrich_companies: bool = True


@app.post("/enrich/scrape")
async def enrich_scrape(body: ScrapeBody):
    """Scrape URL with AI extractor, enrich with Apollo, save to DB."""
    if not config.APIFY_API_TOKEN:
        raise HTTPException(
            status_code=503,
            detail="APIFY_API_TOKEN is not set. Set it to use website scraping.",
        )
    if not config.APOLLO_API_KEY:
        raise HTTPException(
            status_code=503,
            detail="APOLLO_API_KEY is not set. Required for enriching scraped contacts.",
        )
    if not body.url or not body.url.strip():
        raise HTTPException(status_code=400, detail="URL is required.")
    try:
        results = services.run_scrape_core(
            body.url,
            enrich_people=body.enrich_people,
            enrich_companies=body.enrich_companies,
        )
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


# ---------------------------------------------------------------------------
# Base upload
# ---------------------------------------------------------------------------


@app.post("/upload/base")
async def upload_base(
    file: UploadFile = File(...),
    db: DatabaseManager = Depends(get_db),
):
    """Upload base Excel data to database without enrichment."""
    if not file.filename or not file.filename.lower().endswith(".xlsx"):
        raise HTTPException(status_code=400, detail="File must be an .xlsx Excel file.")
    try:
        file_bytes = await file.read()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to read file: {e}") from e
    try:
        data = services.upload_base_data_core(file_bytes, db=db)
        return data
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


# ---------------------------------------------------------------------------
# Database routes
# ---------------------------------------------------------------------------


@app.get("/db/stats")
async def db_stats(db: DatabaseManager = Depends(get_db)):
    """Return database statistics."""
    try:
        return db.get_statistics()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


@app.get("/db/columns")
async def db_columns(db: DatabaseManager = Depends(get_db)):
    """Return column lists: base and apollo."""
    try:
        columns = db.get_column_list()
        base = [c for c in columns if not c.startswith("Apollo")]
        apollo = sorted([c for c in columns if c.startswith("Apollo")])
        return {"base": base, "apollo": apollo}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


@app.get("/db/records")
async def db_records(
    db: DatabaseManager = Depends(get_db),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    email: Optional[str] = Query(None),
    company: Optional[str] = Query(None),
    country: Optional[str] = Query(None),
    first_name: Optional[str] = Query(None),
    last_name: Optional[str] = Query(None),
    job_title: Optional[str] = Query(None),
    industry: Optional[str] = Query(None),
    state: Optional[str] = Query(None),
    website: Optional[str] = Query(None),
    lead_source: Optional[str] = Query(None),
    client_type: Optional[str] = Query(None),
    email_send: Optional[str] = Query(None),
):
    """Search records with filters and pagination."""
    try:
        filters = _build_filters(
            email=email,
            company=company,
            country=country,
            first_name=first_name,
            last_name=last_name,
            job_title=job_title,
            industry=industry,
            state=state,
            website=website,
            lead_source=lead_source,
            client_type=client_type,
            email_send=email_send,
        )
        records, total = db.search_records(filters=filters, limit=limit, offset=offset)
        return {"records": records, "total": total}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


@app.get("/db/export")
async def db_export(
    db: DatabaseManager = Depends(get_db),
    email: Optional[str] = Query(None),
    company: Optional[str] = Query(None),
    country: Optional[str] = Query(None),
    first_name: Optional[str] = Query(None),
    last_name: Optional[str] = Query(None),
    job_title: Optional[str] = Query(None),
    industry: Optional[str] = Query(None),
    state: Optional[str] = Query(None),
    website: Optional[str] = Query(None),
    lead_source: Optional[str] = Query(None),
    client_type: Optional[str] = Query(None),
    email_send: Optional[str] = Query(None),
):
    """Export filtered data as Excel file."""
    try:
        filters = _build_filters(
            email=email,
            company=company,
            country=country,
            first_name=first_name,
            last_name=last_name,
            job_title=job_title,
            industry=industry,
            state=state,
            website=website,
            lead_source=lead_source,
            client_type=client_type,
            email_send=email_send,
        )
        file_bytes, filename = services.export_to_excel(filters, db=db)
        return Response(
            content=file_bytes,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


@app.get("/health")
async def health():
    """Health check."""
    return {"status": "ok"}
