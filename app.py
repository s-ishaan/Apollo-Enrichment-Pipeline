"""
Apollo Enrichment Pipeline — UI has moved to FastAPI backend + Next.js frontend.

Run the backend:   uvicorn backend.main:app --reload
Run the frontend:  cd frontend && npm run dev

See README.md for full setup and environment variables.
"""

if __name__ == "__main__":
    print(
        "Apollo Enrichment Pipeline uses FastAPI backend + Next.js frontend.\n"
        "Backend:  uvicorn backend.main:app --reload\n"
        "Frontend: cd frontend && npm run dev\n"
        "See README.md for details."
    )
