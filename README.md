# Apollo Enrichment Pipeline

A Python-based "single source of truth" data enrichment pipeline with Streamlit UI. Upload Excel files containing organization and people data, enrich them using Apollo.io APIs, and store the results in a SQLite database (local) or PostgreSQL via Supabase (production/cloud).

## Features

- 📤 **Excel Upload**: Upload .xlsx files with company and people information
- 🌐 **Scrape & Enrich**: Enter any URL; an AI extractor (Apify) extracts names and organizations with no selectors, then enriches and saves to the database
- 🔄 **Data Enrichment**: Enrich people and organization data via Apollo.io APIs
- 💾 **Database**: SQLite locally; optional PostgreSQL (Supabase) for production and Streamlit Cloud
- 🔍 **Search & Filter**: Search database by email, company, country, etc.
- 📊 **Database Viewer**: View and paginate through all stored records
- 📥 **Export**: Export filtered data to Excel
- 🔒 **PII Protection**: Automatic masking of sensitive data in logs
- ♻️ **Idempotent**: Re-uploading the same file updates existing records without duplicates

## Architecture

```
apollo-db/
├── app.py           # Streamlit UI
├── config.py        # Configuration management
├── db.py            # Database layer (SQLite + optional PostgreSQL/Supabase)
├── apollo.py        # Apollo API client with retry logic
├── ingest.py        # Excel ingestion & enrichment orchestration
├── scraper.py       # Apify AI extractor (no-selector website scraping)
├── utils.py         # Utility functions (PII masking, validation, etc.)
├── requirements.txt # Python dependencies
└── .env             # Environment variables (not committed)
```

## Prerequisites

- Python 3.8 or higher
- Apollo.io API key ([Get one here](https://app.apollo.io/))

## Installation

1. **Clone or download this repository**

2. **Create a virtual environment** (recommended):
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Set up environment variables**:
   ```bash
   cp .env.example .env
   ```

   Edit `.env` and add your Apollo API key (and optionally Apify token for Scrape & Enrich):
   ```
   APOLLO_API_KEY=your_actual_api_key_here
   # For Scrape & Enrich (optional):
   # APIFY_API_TOKEN=your_apify_token  # https://console.apify.com/settings/integrations
   ```

## Usage

### Running the Application

Start the Streamlit app:

```bash
streamlit run app.py
```

The application will open in your browser at `http://localhost:8501`.

### Scrape & Enrich (any URL)

1. Set `APIFY_API_TOKEN` in `.env` (get it from [Apify Console](https://console.apify.com/settings/integrations)).
2. Navigate to **"Scrape & Enrich"** in the sidebar.
3. Enter a page URL (e.g. a team or contact page). No selectors or page structure required—works for any link.
4. Optionally enable/disable **Enrich People Data** and **Enrich Company Data**.
5. Click **"Run Scrape & Enrich"**. The AI extractor will pull names and organizations from the page, then Apollo will enrich and save only rows that get an email.

Scraped records use Lead Source **"Website Scrape"**; filter by that in the Database Viewer.

### Uploading Data

1. Navigate to the **"Upload & Enrich"** page
2. Click "Browse files" and select an Excel file (.xlsx)
3. Preview the data to verify it was parsed correctly
4. Select enrichment options:
   - ✅ Enrich People Data
   - ✅ Enrich Company Data
5. Click "Start Enrichment"
6. Wait for the process to complete and view the results

### Excel File Format

Your Excel file should contain columns with company and people information. The system automatically detects columns using flexible matching (case-insensitive):

**Supported Column Names**:

- **Email**: "email", "email address", "e-mail", "email id"
- **First Name**: "first name", "firstname", "fname"
- **Last Name**: "last name", "lastname", "lname"
- **Company**: "company", "organization", "org", "company name"
- **Website**: "website", "domain", "url", "website url"
- **Job Title**: "title", "job title", "position", "role"
- **Phone**: "phone", "phone number", "contact number"
- **Country**: "country"
- **State**: "state", "province", "region"

**Example Excel Structure**:
| Email | First Name | Last Name | Company | Website |
|----------------------|------------|-----------|-----------------|-------------------|
| john@example.com | John | Doe | Example Inc | example.com |
| jane@acme.org | Jane | Smith | ACME Corp | acme.org |

### Viewing the Database

1. Navigate to the **"Database Viewer"** page
2. Use search filters to find specific records
3. Use pagination controls to browse through records
4. Click "Export to Excel" to download the data

### Exporting Data

1. In the Database Viewer, apply any desired filters
2. Click "Export to Excel"
3. Click "Download Excel File" to save the exported data

## Production / Streamlit Cloud

To run on Streamlit Community Cloud (or any host without persistent disk), use **PostgreSQL (Supabase)** as the database so data persists across restarts.

1. Create a project in [Supabase](https://supabase.com/) and open **Project Settings → Database**.
2. Copy the **Connection string (URI)** in **Session** mode (recommended for long-lived connections).
3. In your Streamlit app’s **Secrets**, set:
   ```toml
   DATABASE_URL = "postgresql://postgres.[PROJECT_REF]:[PASSWORD]@...pooler.supabase.com:6543/postgres?sslmode=require"
   ```
   Use the exact URI from the Supabase dashboard (it may already include `?sslmode=require`).
4. Deploy the app. When `DATABASE_URL` is set, the app uses PostgreSQL instead of SQLite; do not set `DB_PATH` for production.

Local development can keep using SQLite by leaving `DATABASE_URL` unset.

## Backups

- **Supabase (Pro and above):** Daily automated backups with 7-day retention (Team: 14 days; Enterprise: up to 30 days). Manage and restore from **Supabase Dashboard → Database → Backups**. The free tier does not include automatic backups; for a long-term source of truth, use at least Pro or rely on manual export.
- **Point-in-Time Recovery (PITR):** Optional add-on for Pro for restore to any second within the retention window.
- **Application-level:** Use **Export to Excel** in the Database Viewer (full or filtered) as a secondary backup. Restore by re-importing the Excel via Upload & Enrich or Upload Base Data.

## Database Schema

### Base Columns (22 predefined)

The Truth table contains these exact columns:

1. S.N.
2. Company Name (Based on Website Domain)
3. Industry
4. Revenue
5. Size
6. Company Address / Headquarters
7. Contact Number (Company)
8. Listed Company
9. Website URLs
10. LinkedIn Company Page
11. # Employees
12. First Name
13. Last Name
14. Job Title
15. Email ID (unique) ← **Unique key**
16. Person LinkedIn Profile
17. Contact Number (Person)
18. Country
19. State
20. Lead Source
21. Client Type
22. UPDATE AS ON
23. Email Send (Yes/No)

### Dynamic Apollo Columns

Additional columns are automatically added when Apollo API returns enrichment data. All Apollo columns are prefixed with either:

- `Apollo Person: ...` (e.g., "Apollo Person: Seniority", "Apollo Person: Headline")
- `Apollo Company: ...` (e.g., "Apollo Company: Founded Year", "Apollo Company: Logo URL")

Examples:
- Apollo Person: Email Status
- Apollo Person: Departments
- Apollo Company: Primary Domain
- Apollo Company: Founded Year
- Apollo Company: Keywords

## Configuration

All configuration can be customized via environment variables in the `.env` file:

### Database Configuration
- `DB_PATH`: SQLite file path when `DATABASE_URL` is not set (default: apollo_truth.db)
- `DATABASE_URL`: When set (e.g. Supabase URI), the app uses PostgreSQL instead of SQLite. Omit for local dev.

### API Configuration
- `APOLLO_API_KEY`: Your Apollo.io API key (**required**)
- `APOLLO_BATCH_SIZE`: Records per API call (default: 25)
- `APOLLO_TIMEOUT`: Request timeout in seconds (default: 30)
- `APOLLO_MAX_RETRIES`: Max retry attempts (default: 5)

### Processing Configuration
- `MAX_FILE_SIZE_MB`: Maximum Excel file size (default: 50)
- `ENABLE_PEOPLE_ENRICHMENT`: Enable people enrichment (default: true)
- `ENABLE_COMPANY_ENRICHMENT`: Enable company enrichment (default: true)

### Logging Configuration
- `LOG_LEVEL`: Logging level (DEBUG, INFO, WARNING, ERROR)
- `LOG_FILE`: Log file path (default: apollo_pipeline.log)
- `LOG_TO_CONSOLE`: Log to console (default: true)

## Features in Detail

### Idempotent Upserts

The system uses the **Email ID (unique)** column as the primary key:
- **New records**: Inserted with auto-incremented S.N.
- **Existing records**: Updated with preserved S.N. and new timestamp
- Safe to re-run the same file multiple times

### Retry Logic

Apollo API calls include robust retry logic:
- **Rate limiting (429)**: Exponential backoff (1s, 2s, 4s, 8s, 16s, max 60s)
- **Server errors (5xx)**: Retry up to 5 times
- **Timeouts**: Retry with increased timeout
- **Client errors (4xx)**: Fail immediately with clear error message

### PII Protection

All logs automatically mask sensitive information:
- **Emails**: user@example.com → u***@e***.com
- **Phone numbers**: +1-234-567-8900 → +*-***-***-**00
- **API key**: Never logged

### Partial Enrichment

The system handles partial failures gracefully:
- Continue processing even if some records fail
- Store base data even if enrichment fails
- Track and report all errors
- Users can re-run enrichment for failed records

## Troubleshooting

### "APOLLO_API_KEY not set" Error

Make sure you've created a `.env` file with your API key:
```bash
APOLLO_API_KEY=your_key_here
```

### Excel File Not Parsing

- Ensure the file is a valid `.xlsx` format
- Check that the file isn't corrupted
- Verify the file size is under the limit (default: 50MB)

### No Data After Enrichment

- Check the logs for error messages (`apollo_pipeline.log`)
- Verify your Apollo API key is valid and has credits
- Check network connectivity

### Database Locked Errors

If you see "database is locked" errors (SQLite only):
- Close any other applications accessing the database
- Restart the Streamlit app
- Check file permissions

### DATABASE_URL / PostgreSQL Connection Errors

If you set `DATABASE_URL` and the app fails to start or connect:
- Ensure the URI is the **Session**-mode connection string from Supabase (not Transaction mode).
- Include `?sslmode=require` in the URI if your host requires SSL (Supabase usually does).
- Verify the password and that the database is reachable from your network (e.g. Streamlit Cloud).
- Check logs for the exact error; `psycopg2.OperationalError` often indicates a bad URI, wrong credentials, or firewall.

## Development

### Running Tests

```bash
# Install test dependencies
pip install -r requirements.txt

# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=. --cov-report=term-missing
```

### Adding New Features

The codebase is modular:
- **utils.py**: Add utility functions
- **db.py**: Modify database operations
- **apollo.py**: Add new API endpoints or mapping logic
- **ingest.py**: Modify enrichment pipeline
- **app.py**: Add new UI features

## Security Best Practices

- ✅ Never commit `.env` file or secrets
- ✅ All PII is masked in logs
- ✅ API key loaded from environment variable only
- ✅ Parameterized SQL queries prevent injection
- ✅ Input validation on all user inputs

## License

This project is proprietary software. All rights reserved.

## Support

For issues, questions, or feature requests, please contact your system administrator.

## Changelog

### Version 1.0.0 (2026-01-29)
- Initial release
- Excel upload and enrichment
- Apollo API integration (people & organizations)
- SQLite database with dynamic schema
- Streamlit UI with search and export
- PII masking and security features
- Comprehensive error handling and logging
