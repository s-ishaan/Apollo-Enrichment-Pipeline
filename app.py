"""
Streamlit UI for Apollo enrichment pipeline.
Provides file upload, enrichment, database viewing, and export functionality.
"""

import io
import os
import tempfile
from datetime import datetime
import streamlit as st
import pandas as pd

from config import config, load_config
from db import DatabaseManager
from apollo import ApolloClient
from ingest import ExcelIngestor, process_scrape
from utils import setup_logging, mask_pii

# Setup logging
logger = setup_logging(config.LOG_LEVEL, config.LOG_FILE, config.LOG_TO_CONSOLE)

# Page configuration
st.set_page_config(
    page_title=config.STREAMLIT_PAGE_TITLE,
    page_icon="📊",
    layout="wide"
)

NAV_OPTIONS = [
    "📤 Upload & Enrich",
    "🌐 Scrape & Enrich",
    "📥 Upload Base Data",
    "🗄️ Database Viewer",
]


def main():
    """
    Main application entry point.
    """
    # Session state: lock sidebar while any enrichment/upload process is running
    if "enrichment_in_progress" not in st.session_state:
        st.session_state.enrichment_in_progress = False
    if "enrichment_page_locked" not in st.session_state:
        st.session_state.enrichment_page_locked = NAV_OPTIONS[0]
    if "pending_enrichment" not in st.session_state:
        st.session_state.pending_enrichment = None

    st.title("📊 Apollo Enrichment Pipeline")

    # Sidebar navigation (disabled while a process is in progress)
    in_progress = st.session_state.enrichment_in_progress
    if in_progress:
        page = st.session_state.enrichment_page_locked
        nav_index = NAV_OPTIONS.index(page) if page in NAV_OPTIONS else 0
        st.sidebar.radio(
            "Navigation",
            NAV_OPTIONS,
            index=nav_index,
            disabled=True,
            key="nav_radio",
        )
        st.sidebar.caption("⏳ Enrichment in progress…")
    else:
        page = st.sidebar.radio(
            "Navigation",
            NAV_OPTIONS,
            index=0,
            key="nav_radio",
        )

    # API key check (only for enrichment pages)
    if page == "📤 Upload & Enrich" and not config.APOLLO_API_KEY:
        st.error(
            "⚠️ APOLLO_API_KEY environment variable not set. "
            "Please set it before using enrichment."
        )
        st.stop()

    if page == "🌐 Scrape & Enrich":
        if not config.APIFY_API_TOKEN:
            st.error(
                "⚠️ APIFY_API_TOKEN environment variable not set. "
                "Set it in .env to use website scraping (get token at "
                "https://console.apify.com/settings/integrations)."
            )
            st.stop()
        if not config.APOLLO_API_KEY:
            st.error(
                "⚠️ APOLLO_API_KEY environment variable not set. "
                "Required for enriching scraped contacts."
            )
            st.stop()

    # Route to appropriate page
    if page == "📤 Upload & Enrich":
        render_upload_page()
    elif page == "🌐 Scrape & Enrich":
        render_scrape_page()
    elif page == "📥 Upload Base Data":
        render_base_upload_page()
    else:
        render_database_page()


def _run_pending_upload_enrichment():
    """Run upload enrichment from session state (after sidebar lock rerun). Returns results or None."""
    buf = io.BytesIO(st.session_state._pending_upload_bytes)
    buf.name = st.session_state.get("_pending_upload_name", "upload.xlsx")
    return run_enrichment_pipeline(
        buf,
        st.session_state._pending_enrich_people,
        st.session_state._pending_enrich_companies,
    )


def render_upload_page():
    """
    Page 1: File upload and enrichment.
    """
    # Run pending process (after rerun with sidebar disabled)
    if st.session_state.get("pending_enrichment") == "upload":
        st.session_state.enrichment_in_progress = True
        results = _run_pending_upload_enrichment()
        st.session_state.pending_enrichment = None
        st.session_state.enrichment_in_progress = False
        if results is not None:
            st.session_state._last_upload_result = {
                "results": results,
                "enrich_people": st.session_state._pending_enrich_people,
                "enrich_companies": st.session_state._pending_enrich_companies,
            }
        st.rerun()

    # After rerun: show last success so sidebar is updated and message stays
    if st.session_state.get("_last_upload_result"):
        data = st.session_state._last_upload_result
        _render_upload_success(
            data["results"],
            data["enrich_people"],
            data["enrich_companies"],
        )
        del st.session_state._last_upload_result
        st.divider()

    st.header("📤 Upload & Enrich Data")

    st.markdown("""
    Upload an Excel file (.xlsx) containing organization and people data.
    The system will enrich the data using Apollo API and store it in the database.
    """)

    # File upload widget
    uploaded_file = st.file_uploader(
        "Choose an Excel file",
        type=["xlsx"],
        help="Upload an Excel file with company and people information"
    )

    if uploaded_file:
        # Preview data
        with st.expander("📋 Preview Data", expanded=True):
            try:
                df_preview = pd.read_excel(uploaded_file, nrows=10)
                st.dataframe(df_preview, use_container_width=True)

                # Reset file pointer for later processing
                uploaded_file.seek(0)

                # Show file info
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Rows (preview)", len(df_preview))
                with col2:
                    st.metric("Columns", len(df_preview.columns))

            except Exception as e:
                st.error(f"Failed to preview file: {e}")
                return

        # Enrichment options
        st.subheader("🔧 Enrichment Options")

        col1, col2 = st.columns(2)
        with col1:
            enrich_people = st.checkbox(
                "Enrich People Data",
                value=True,
                help="Enrich contact information using Apollo People API"
            )
        with col2:
            enrich_companies = st.checkbox(
                "Enrich Company Data",
                value=True,
                help="Enrich organization information using Apollo Organizations API"
            )

        # Run enrichment button: lock sidebar and rerun, then process runs at top of next run
        if st.button("🚀 Start Enrichment", type="primary", use_container_width=True):
            st.session_state._pending_upload_bytes = uploaded_file.getvalue()
            st.session_state._pending_upload_name = (
                os.path.basename(uploaded_file.name) if uploaded_file.name else "upload.xlsx"
            )
            st.session_state._pending_enrich_people = enrich_people
            st.session_state._pending_enrich_companies = enrich_companies
            st.session_state.pending_enrichment = "upload"
            st.session_state.enrichment_in_progress = True
            st.session_state.enrichment_page_locked = "📤 Upload & Enrich"
            st.rerun()


def _render_upload_success(
    results: dict,
    enrich_people: bool,
    enrich_companies: bool,
) -> None:
    """Render success metrics and errors for upload enrichment (after rerun)."""
    empty_reason = results.get("empty_reason")
    if empty_reason:
        st.info(
            "No data rows to process. The file was empty, had only headers, "
            "or all rows were removed (e.g. missing or invalid email)."
        )
    else:
        saved = results.get("new_inserts", 0) + results.get("updates", 0)
        skipped = results.get("skipped_no_email", 0)
        if saved == 0 and skipped > 0:
            st.warning(
                f"No records were saved. All {skipped} row(s) were skipped (no valid email). "
                "Ensure your Excel has an email column with valid addresses."
            )
        else:
            st.success("🎉 Enrichment completed successfully!")

    for w in results.get("warnings", []):
        st.warning(w)

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Processed", results.get("total_processed", 0))
    with col2:
        st.metric("New Records", results.get("new_inserts", 0))
    with col3:
        st.metric("Updated Records", results.get("updates", 0))
    with col4:
        st.metric("Failed", results.get("failed", 0))  # only DB upsert failures

    if enrich_people or enrich_companies:
        col5, col6 = st.columns(2)
        with col5:
            st.metric("People Enriched", results.get("people_enriched", 0))
        with col6:
            st.metric("Companies Enriched", results.get("orgs_enriched", 0))

    skipped = results.get("skipped_no_email", 0)
    if skipped > 0:
        st.caption(f"Skipped (no email): {skipped} record(s)")

    org_skipped = results.get("org_enrichment_skipped_no_domain", 0)
    if org_skipped > 0 and enrich_companies:
        st.caption(f"Company enrichment skipped (no website): {org_skipped} row(s)")

    updated_emails = results.get("updated_emails", [])
    if updated_emails:
        with st.expander(f"📝 Updated records ({len(updated_emails)})", expanded=True):
            for email in updated_emails:
                st.text(f"• {mask_pii(email)}")

    inserted_emails = results.get("inserted_emails", [])
    if inserted_emails:
        with st.expander(f"✨ New records ({len(inserted_emails)})", expanded=False):
            for email in inserted_emails:
                st.text(f"• {mask_pii(email)}")

    failed_records = results.get("failed_records", [])
    if failed_records:
        with st.expander(f"⚠️ Failed to save ({len(failed_records)})", expanded=True):
            for item in failed_records:
                st.error(f"**{mask_pii(item.get('email', ''))}**: {item.get('error', 'Unknown error')}")

    if results.get("errors"):
        with st.expander("⚠️ Errors Encountered", expanded=False):
            for error in results["errors"]:
                st.error(f"**{error['email']}**: {error['message']}")


def _show_structured_pipeline_error(exc: Exception) -> None:
    """Show a structured error message for pipeline failures."""
    err_msg = str(exc)
    if isinstance(exc, ValueError):
        if "too large" in err_msg.lower() or "max:" in err_msg.lower():
            st.error(f"❌ File error: {err_msg}")
        elif "email" in err_msg.lower() or "column" in err_msg.lower():
            st.error(f"❌ Data error: {err_msg}")
        else:
            st.error(f"❌ Validation error: {err_msg}")
    elif "sqlite" in type(exc).__module__ or "sqlite3" in type(exc).__name__.lower():
        st.error(f"❌ Database error: {err_msg}")
    elif "request" in type(exc).__module__ or "ConnectionError" in type(exc).__name__:
        st.error(f"❌ API/network error: {err_msg}")
    else:
        st.error(f"❌ Enrichment failed: {err_msg}")


def run_enrichment_pipeline(
    uploaded_file,
    enrich_people: bool,
    enrich_companies: bool,
):
    """
    Execute enrichment pipeline with progress tracking.
    Returns results dict on success, None on failure (caller displays success after rerun).
    """
    progress_bar = st.progress(0)
    status_text = st.empty()
    tmp_file_path: str | None = None

    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp_file:
            tmp_file.write(uploaded_file.getvalue())
            tmp_file_path = tmp_file.name

        status_text.text("Initializing...")
        try:
            load_config(validate=True)
        except ValueError as ve:
            progress_bar.empty()
            status_text.empty()
            st.error(f"❌ Configuration error: {ve}")
            return None
        db = DatabaseManager()
        apollo = ApolloClient()
        ingestor = ExcelIngestor(db, apollo)

        def update_progress(stage: str, current: int, total: int):
            progress = current / total if total > 0 else 0
            progress_bar.progress(progress)
            status_text.text(f"{stage}... ({int(progress * 100)}%)")

        results = ingestor.process_file(
            tmp_file_path,
            enrich_people=enrich_people,
            enrich_companies=enrich_companies,
            progress_callback=update_progress,
        )

        progress_bar.progress(1.0)
        status_text.text("✅ Enrichment Complete!")

        db.close()
        return results

    except Exception as e:
        progress_bar.empty()
        status_text.empty()
        _show_structured_pipeline_error(e)
        logger.exception("Enrichment pipeline failed")
        return None
    finally:
        if tmp_file_path and os.path.isfile(tmp_file_path):
            try:
                os.unlink(tmp_file_path)
            except OSError as cleanup_err:
                logger.warning("Failed to remove temp file %s: %s", tmp_file_path, cleanup_err)


def _render_scrape_success(
    results: dict,
    enrich_people: bool,
    enrich_companies: bool,
) -> None:
    """Render success metrics and details for scrape (after rerun)."""
    if results.get("errors") and not results.get("total_processed") and not results.get("new_inserts") and not results.get("updates"):
        st.error("Scrape or enrichment failed. See errors below.")
    else:
        st.success("Scrape & enrich finished.")

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Processed", results.get("total_processed", 0))
    with col2:
        st.metric("New Records", results.get("new_inserts", 0))
    with col3:
        st.metric("Updated Records", results.get("updates", 0))
    with col4:
        st.metric("Failed", results.get("failed", 0))

    if enrich_people or enrich_companies:
        col5, col6 = st.columns(2)
        with col5:
            st.metric("People Enriched", results.get("people_enriched", 0))
        with col6:
            st.metric("Companies Enriched", results.get("orgs_enriched", 0))

    saved_records = results.get("saved_records", [])
    if saved_records:
        st.info(f"Saved to database: {len(saved_records)} record(s)")
        with st.expander("View names and organizations (saved to database)", expanded=False):
            for rec in saved_records:
                name = (
                    " ".join(
                        filter(
                            None,
                            [rec.get("First Name", ""), rec.get("Last Name", "")],
                        )
                    ).strip()
                    or "—"
                )
                org = (rec.get("Company Name (Based on Website Domain)") or "").strip() or "—"
                st.text(f"• {name} — {org}")

    skipped = results.get("skipped_no_email", 0)
    if skipped > 0:
        st.info(f"Skipped (no email after enrichment): {skipped}")
        skipped_records = results.get("skipped_no_email_records", [])
        if skipped_records:
            with st.expander("View names and organizations (no email found)", expanded=False):
                for rec in skipped_records:
                    name = (
                        " ".join(
                            filter(None, [rec.get("First Name", ""), rec.get("Last Name", "")])
                        ).strip()
                        or "—"
                    )
                    org = (rec.get("Company Name (Based on Website Domain)") or "").strip() or "—"
                    st.text(f"• {name} — {org}")

    if results.get("errors"):
        with st.expander("⚠️ Errors Encountered", expanded=False):
            for err in results["errors"]:
                st.error(f"**{err.get('email', 'N/A')}**: {err.get('message', '')}")


def _run_scrape_and_show_results(url: str, enrich_people: bool, enrich_companies: bool):
    """Run process_scrape with progress; returns results dict or None (success shown after rerun)."""
    progress_bar = st.progress(0)
    status_text = st.empty()

    def update_progress(stage: str, current: int, total: int):
        progress = current / total if total > 0 else 0
        progress_bar.progress(progress)
        status_text.text(f"{stage}... ({int(progress * 100)}%)")

    try:
        results = process_scrape(
            url.strip(),
            enrich_people=enrich_people,
            enrich_companies=enrich_companies,
            progress_callback=update_progress,
        )
        progress_bar.progress(1.0)
        status_text.text("✅ Complete!")
        return results
    except Exception as e:
        progress_bar.empty()
        status_text.empty()
        st.error(f"❌ Scrape failed: {str(e)}")
        logger.exception("Scrape page failed")
        return None


def render_scrape_page():
    """
    Page: Scrape a website with AI extractor (no selectors), then enrich and save.
    """
    # Run pending scrape (after rerun with sidebar disabled)
    if st.session_state.get("pending_enrichment") == "scrape":
        st.session_state.enrichment_in_progress = True
        results = _run_scrape_and_show_results(
            st.session_state._pending_scrape_url,
            st.session_state._pending_scrape_enrich_people,
            st.session_state._pending_scrape_enrich_companies,
        )
        st.session_state.pending_enrichment = None
        st.session_state.enrichment_in_progress = False
        if results is not None:
            st.session_state._last_scrape_result = {
                "results": results,
                "enrich_people": st.session_state._pending_scrape_enrich_people,
                "enrich_companies": st.session_state._pending_scrape_enrich_companies,
            }
        st.rerun()

    # After rerun: show last success so sidebar is updated and message stays
    if st.session_state.get("_last_scrape_result"):
        data = st.session_state._last_scrape_result
        _render_scrape_success(
            data["results"],
            data["enrich_people"],
            data["enrich_companies"],
        )
        del st.session_state._last_scrape_result
        st.divider()

    st.header("🌐 Scrape & Enrich")

    st.markdown("""
    Enter a URL to extract person names and organizations from the page using an AI extractor.
    No selectors or page structure required—works for any link. Extracted contacts are then
    enriched via Apollo and saved to the database (only rows with an email after enrichment are stored).
    """)

    url = st.text_input(
        "Page URL",
        placeholder="https://example.com/team",
        help="Any URL that contains names and optionally organizations",
        key="scrape_url",
    )

    col1, col2 = st.columns(2)
    with col1:
        enrich_people = st.checkbox(
            "Enrich People Data",
            value=True,
            help="Enrich contacts using Apollo People API",
            key="scrape_enrich_people",
        )
    with col2:
        enrich_companies = st.checkbox(
            "Enrich Company Data",
            value=True,
            help="Enrich organizations using Apollo Organizations API",
            key="scrape_enrich_companies",
        )

    if st.button("🚀 Run Scrape & Enrich", type="primary", use_container_width=True):
        if not url or not url.strip():
            st.warning("Please enter a URL.")
            return

        st.session_state._pending_scrape_url = url.strip()
        st.session_state._pending_scrape_enrich_people = enrich_people
        st.session_state._pending_scrape_enrich_companies = enrich_companies
        st.session_state.pending_enrichment = "scrape"
        st.session_state.enrichment_in_progress = True
        st.session_state.enrichment_page_locked = "🌐 Scrape & Enrich"
        st.rerun()


def render_base_upload_page():
    """
    Page for uploading base Excel data without Apollo enrichment.
    """
    # Run pending base upload (after rerun with sidebar disabled)
    if st.session_state.get("pending_enrichment") == "base_upload":
        st.session_state.enrichment_in_progress = True
        buf = io.BytesIO(st.session_state._pending_base_bytes)
        buf.name = st.session_state.get("_pending_base_name", "upload.xlsx")
        result = upload_base_data(buf)
        st.session_state.pending_enrichment = None
        st.session_state.enrichment_in_progress = False
        if result is not None:
            st.session_state._last_base_result = result
        st.rerun()

    # After rerun: show last success so sidebar is updated and message stays
    if st.session_state.get("_last_base_result"):
        data = st.session_state._last_base_result
        _render_base_upload_success(data["stats"], data["total"])
        del st.session_state._last_base_result
        st.divider()

    st.header("📥 Upload Base Data")

    st.markdown("""
    Upload your existing Excel data directly to the database **without Apollo enrichment**.

    This is useful for:
    - 📂 Importing your existing contact database
    - 🔄 Bulk uploading historical data
    - 📊 Merging data from other sources

    **Note:** This will NOT call Apollo API - data is loaded as-is from your Excel file.
    """)

    # File upload widget
    uploaded_file = st.file_uploader(
        "Choose an Excel file (.xlsx)",
        type=["xlsx"],
        help="Upload Excel file with your base data",
        key="base_upload"
    )

    if uploaded_file:
        # Preview data
        with st.expander("📋 Preview Data", expanded=True):
            try:
                df_preview = pd.read_excel(uploaded_file, nrows=10)
                st.dataframe(df_preview, use_container_width=True)

                # Reset file pointer
                uploaded_file.seek(0)

                # Show file info
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Rows (preview)", len(df_preview))
                with col2:
                    st.metric("Columns", len(df_preview.columns))

            except Exception as e:
                st.error(f"Failed to preview file: {e}")
                return

        # Upload button: lock sidebar and rerun, then process runs at top of next run
        if st.button("📥 Upload to Database", type="primary", use_container_width=True):
            st.session_state._pending_base_bytes = uploaded_file.getvalue()
            st.session_state._pending_base_name = uploaded_file.name
            st.session_state.pending_enrichment = "base_upload"
            st.session_state.enrichment_in_progress = True
            st.session_state.enrichment_page_locked = "📥 Upload Base Data"
            st.rerun()


def _render_base_upload_success(stats: dict, total: int) -> None:
    """Render success metrics for base upload (after rerun)."""
    st.success("🎉 Base data uploaded successfully!")

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Processed", total)
    with col2:
        st.metric("New Records", stats["inserted"])
    with col3:
        st.metric("Updated Records", stats["updated"])

    if stats.get("failed", 0) > 0:
        st.warning(f"⚠️ {stats['failed']} records failed to upload")


def upload_base_data(uploaded_file):
    """
    Upload base Excel data directly to database without enrichment.
    Returns dict with stats and total count on success, None on failure (success shown after rerun).
    """
    progress_bar = st.progress(0)
    status_text = st.empty()
    tmp_file_path = None

    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp_file:
            tmp_file.write(uploaded_file.getvalue())
            tmp_file_path = tmp_file.name

        status_text.text("Initializing...")
        db = DatabaseManager()
        ingestor = ExcelIngestor(db, None)

        def update_progress(stage: str, current: int, total: int):
            progress = current / total if total > 0 else 0
            progress_bar.progress(progress)
            status_text.text(f"{stage}... ({int(progress * 100)}%)")

        update_progress("Parsing Excel", 10, 100)
        df = ingestor.parse_excel(tmp_file_path)

        update_progress("Normalizing data", 30, 100)
        df = ingestor.normalize_dataframe(df)

        update_progress("Deduplicating records", 50, 100)
        df = ingestor.deduplicate_by_email(df)

        update_progress("Saving to database", 70, 100)
        stats = ingestor.save_to_database(df)

        progress_bar.progress(1.0)
        status_text.text("✅ Upload Complete!")

        db.close()
        return {"stats": stats, "total": len(df)}

    except Exception as e:
        progress_bar.empty()
        status_text.empty()
        _show_structured_pipeline_error(e)
        logger.exception("Base data upload failed")
        return None
    finally:
        if tmp_file_path and os.path.isfile(tmp_file_path):
            try:
                os.unlink(tmp_file_path)
            except OSError as cleanup_err:
                logger.warning("Failed to remove temp file %s: %s", tmp_file_path, cleanup_err)


def render_database_page():
    """
    Page 3: Database viewer with enhanced search and export.
    """
    st.header("🗄️ Database Viewer")

    # Initialize database
    db = DatabaseManager()

    # Database statistics
    stats = db.get_statistics()

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Records", stats['total_records'])
    with col2:
        st.metric("Total Columns", stats['total_columns'])
    with col3:
        st.metric("Recent Updates (7 days)", stats['recent_updates_7_days'])

    st.divider()

    # Search and filter controls
    with st.expander("🔍 Search & Filter", expanded=True):
        # Row 1: Basic filters
        col1, col2, col3 = st.columns(3)

        with col1:
            email_search = st.text_input(
                "Email",
                placeholder="Search by email...",
                key="email_filter"
            )

        with col2:
            company_search = st.text_input(
                "Company Name",
                placeholder="Search by company...",
                key="company_filter"
            )

        with col3:
            country_search = st.text_input(
                "Country",
                placeholder="Search by country...",
                key="country_filter"
            )

        # Row 2: Person filters
        st.markdown("**Person Filters:**")
        col4, col5, col6 = st.columns(3)

        with col4:
            first_name_search = st.text_input(
                "First Name",
                placeholder="Search by first name...",
                key="first_name_filter"
            )

        with col5:
            last_name_search = st.text_input(
                "Last Name",
                placeholder="Search by last name...",
                key="last_name_filter"
            )

        with col6:
            job_title_search = st.text_input(
                "Job Title",
                placeholder="Search by job title...",
                key="job_title_filter"
            )

        # Row 3: Company filters
        st.markdown("**Company Filters:**")
        col7, col8, col9 = st.columns(3)

        with col7:
            industry_search = st.text_input(
                "Industry",
                placeholder="Search by industry...",
                key="industry_filter"
            )

        with col8:
            state_search = st.text_input(
                "State",
                placeholder="Search by state...",
                key="state_filter"
            )

        with col9:
            website_search = st.text_input(
                "Website",
                placeholder="Search by website...",
                key="website_filter"
            )

        # Row 4: Lead source and status
        st.markdown("**Lead Filters:**")
        col10, col11, col12 = st.columns(3)

        with col10:
            lead_source_search = st.text_input(
                "Lead Source",
                placeholder="Search by lead source...",
                key="lead_source_filter"
            )

        with col11:
            client_type_search = st.text_input(
                "Client Type",
                placeholder="Search by client type...",
                key="client_type_filter"
            )

        with col12:
            email_send_filter = st.selectbox(
                "Email Send Status",
                ["All", "Yes", "No"],
                key="email_send_filter"
            )

    # Build filters
    filters = {}
    if email_search:
        filters["Email ID (unique)"] = email_search
    if company_search:
        filters["Company Name (Based on Website Domain)"] = company_search
    if country_search:
        filters["Country"] = country_search
    if first_name_search:
        filters["First Name"] = first_name_search
    if last_name_search:
        filters["Last Name"] = last_name_search
    if job_title_search:
        filters["Job Title"] = job_title_search
    if industry_search:
        filters["Industry"] = industry_search
    if state_search:
        filters["State"] = state_search
    if website_search:
        filters["Website URLs"] = website_search
    if lead_source_search:
        filters["Lead Source"] = lead_source_search
    if client_type_search:
        filters["Client Type"] = client_type_search
    if email_send_filter != "All":
        filters["Email Send (Yes/No)"] = email_send_filter

    # Pagination controls
    col1, col2, col3 = st.columns([2, 2, 3])

    with col1:
        page_size = st.selectbox(
            "Records per page",
            [25, 50, 100, 500],
            index=1,
            key="page_size"
        )

    with col2:
        # Get total count for pagination
        _, total_count = db.search_records(filters, limit=1, offset=0)
        max_page = max(1, (total_count + page_size - 1) // page_size)

        page_number = st.number_input(
            "Page",
            min_value=1,
            max_value=max_page,
            value=1,
            key="page_number"
        )

    with col3:
        st.markdown(f"**Total matching records:** {total_count}")

    # Fetch data
    offset = (page_number - 1) * page_size

    try:
        records, total_count = db.search_records(
            filters,
            limit=page_size,
            offset=offset
        )

        if records:
            st.info(
                f"Showing records {offset + 1} to "
                f"{min(offset + len(records), total_count)} of {total_count}"
            )

            # Display data
            df = pd.DataFrame(records)

            # Reorder columns: base columns first, then Apollo columns
            base_cols = [col for col in df.columns if not col.startswith("Apollo")]
            apollo_cols = sorted([col for col in df.columns if col.startswith("Apollo")])
            ordered_cols = base_cols + apollo_cols

            df = df[ordered_cols]

            st.dataframe(
                df,
                use_container_width=True,
                height=600
            )

            # Export section
            st.divider()
            col1, col2 = st.columns([1, 3])

            with col1:
                if st.button("📥 Export to Excel", use_container_width=True):
                    export_data(db, filters)

        else:
            st.warning("No records found matching your filters.")

        # Column information
        with st.expander("📋 Column Information"):
            columns = db.get_column_list()

            # Separate base and Apollo columns
            base_cols = [c for c in columns if not c.startswith("Apollo")]
            apollo_cols = [c for c in columns if c.startswith("Apollo")]

            col1, col2 = st.columns(2)

            with col1:
                st.markdown("**Base Columns:**")
                st.write(f"Total: {len(base_cols)}")
                with st.expander("View all base columns"):
                    for col in base_cols:
                        st.text(f"• {col}")

            with col2:
                st.markdown("**Apollo Columns:**")
                st.write(f"Total: {len(apollo_cols)}")
                if apollo_cols:
                    with st.expander("View all Apollo columns"):
                        for col in apollo_cols:
                            st.text(f"• {col}")
                else:
                    st.info("No Apollo columns yet. Upload and enrich data to add them.")

    except Exception as e:
        st.error(f"Failed to fetch records: {e}")
        logger.exception("Database query failed")

    finally:
        db.close()


def export_data(db: DatabaseManager, filters: dict):
    """
    Export data to Excel and provide download button.

    Args:
        db: Database manager
        filters: Current filters
    """
    try:
        # Fetch data
        df = db.export_to_dataframe(filters)

        if len(df) == 0:
            st.warning("No data to export.")
            return

        # Convert to Excel in memory
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Truth Data')

        buffer.seek(0)

        # Generate filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"apollo_export_{timestamp}.xlsx"

        # Download button
        st.download_button(
            label="⬇️ Download Excel File",
            data=buffer,
            file_name=filename,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )

        st.success(f"✅ Ready to download {len(df)} records!")

    except Exception as e:
        st.error(f"Export failed: {e}")
        logger.exception("Export failed")


if __name__ == "__main__":
    main()
