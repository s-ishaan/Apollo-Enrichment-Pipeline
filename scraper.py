"""
Apify-based AI website scraper for Apollo enrichment pipeline.
Uses HTML-first extraction (fetch + LLM) when enabled, with Apify browser actor
as fallback for JS-heavy pages or when HTML extraction returns no items.
"""

import json
import logging
import re
from typing import Any, Optional

import pandas as pd
import requests

from config import config, BASE_COLUMNS
from utils import setup_logging

logger = setup_logging(config.LOG_LEVEL, config.LOG_FILE,
                       config.LOG_TO_CONSOLE)

# User-Agent for HTML fetch (some sites block non-browser requests)
HTML_FETCH_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; rv:109.0) Gecko/20100101 Firefox/115.0"
)


class ScraperError(Exception):
    """Raised when Apify run fails (timeout, actor error, no token, no dataset)."""
    pass


def _normalize_item(item: dict[str, Any]) -> dict[str, Any]:
    """
    Normalize actor output to consistent shape: firstName, lastName, organization.
    Handles various actor output formats (name vs firstName+lastName, etc.).
    """
    normalized: dict[str, Any] = {}
    item_lower = {str(k).lower(): v for k, v in item.items()
                  if v is not None and str(v).strip()}

    # First name
    for key in ("firstname", "first_name", "firstName"):
        if key in item_lower:
            normalized["firstName"] = str(item_lower[key]).strip()
            break
    if "firstName" not in normalized and "name" in item_lower:
        name = str(item_lower["name"]).strip()
        parts = name.split(None, 1)
        normalized["firstName"] = parts[0] if parts else ""
        if len(parts) > 1:
            normalized["lastName"] = parts[1]
    if "firstName" not in normalized:
        normalized["firstName"] = ""

    # Last name
    for key in ("lastname", "last_name", "lastName"):
        if key in item_lower:
            normalized["lastName"] = str(item_lower[key]).strip()
            break
    if "lastName" not in normalized:
        normalized["lastName"] = normalized.get("lastName", "")

    # Organization / company
    for key in ("organization", "organisation", "company", "companyname", "company_name", "org"):
        if key in item_lower:
            normalized["organization"] = str(item_lower[key]).strip()
            break
    if "organization" not in normalized:
        normalized["organization"] = ""

    # Email if present
    for key in ("email", "emailaddress", "email_address"):
        if key in item_lower:
            normalized["email"] = str(item_lower[key]).strip()
            break

    return normalized


def _fetch_html(url: str) -> Optional[str]:
    """
    Fetch page HTML with a browser-like User-Agent and timeout.
    Returns None on failure (network error, timeout, non-2xx).
    """
    try:
        resp = requests.get(
            url,
            timeout=config.HTML_FETCH_TIMEOUT_SECS,
            headers={"User-Agent": HTML_FETCH_USER_AGENT},
        )
        resp.raise_for_status()
        if resp.encoding is None:
            resp.encoding = "utf-8"
        return resp.text
    except Exception as e:
        logger.debug("HTML fetch failed for %s: %s", url, e)
        return None


def _truncate_html(html: str, max_chars: int) -> str:
    """Truncate HTML to stay within LLM context and cost limits."""
    if len(html) <= max_chars:
        return html
    return html[:max_chars] + "\n[... truncated ...]"


def _extract_from_html_with_llm(html: str, url: str) -> list[dict[str, Any]]:
    """
    Send HTML to OpenAI with extraction prompt; parse JSON array and normalize.
    Returns list of normalized items, or empty list on parse/API error.
    """
    try:
        from openai import OpenAI
    except ImportError:
        logger.debug("openai not installed, skipping HTML LLM extraction")
        return []

    if not config.OPENAI_API_KEY:
        return []

    truncated = _truncate_html(html, config.MAX_HTML_CHARS)
    instruction = (
        "From the following HTML, extract all person names and their organization names. "
        "Return a JSON array of objects with keys: firstName, lastName, organization (optional). "
        "If only a full name is available use firstName for the full name and leave lastName empty. "
        "One object per person. Return only the JSON array, no other text or markdown."
    )
    user_content = f"{instruction}\n\n---\n\nHTML:\n{truncated}"

    try:
        client = OpenAI(api_key=config.OPENAI_API_KEY)
        resp = client.chat.completions.create(
            model=config.OPENAI_EXTRACTION_MODEL,
            messages=[
                {"role": "system", "content": "You extract structured data. Reply with a JSON array only."},
                {"role": "user", "content": user_content},
            ],
        )
        text = (resp.choices[0].message.content or "").strip()
        if not text:
            return []
        # Strip optional markdown code fence
        if "```" in text:
            match = re.search(r"```(?:json)?\s*([\s\S]*?)\s*```", text)
            if match:
                text = match.group(1).strip()
        data = json.loads(text)
        if not isinstance(data, list):
            return []
        normalized = []
        for item in data:
            if not isinstance(item, dict):
                continue
            norm = _normalize_item(item)
            if norm.get("firstName") or norm.get("lastName") or norm.get("organization"):
                normalized.append(norm)
        return normalized
    except json.JSONDecodeError as e:
        logger.debug("LLM extraction JSON parse error for %s: %s", url, e)
        return []
    except Exception as e:
        logger.debug("LLM extraction failed for %s: %s", url, e)
        return []


def run_ai_extractor(
    url: str,
    actor_id: Optional[str] = None,
    run_input: Optional[dict[str, Any]] = None,
    timeout_secs: Optional[int] = None,
) -> list[dict[str, Any]]:
    """
    Extract names and organizations from a URL. Tries HTML fetch + LLM first
    (fewer resources); falls back to Apify browser actor if HTML fails or returns no items.

    Args:
        url: Page URL to scrape.
        actor_id: Apify actor ID (defaults to config).
        run_input: Override run input; if None, built from url + config prompt.
        timeout_secs: Run timeout in seconds (defaults to config).

    Returns:
        List of normalized items with keys firstName, lastName, organization, email (optional).

    Raises:
        ScraperError: If tokens missing when needed, run fails, or dataset unavailable.
    """
    if not config.OPENAI_API_KEY:
        raise ScraperError(
            "OPENAI_API_KEY is not set. Set it in .env for website scraping (HTML or Apify)."
        )

    # HTML-first: fetch + LLM extraction (cheap); skip Apify when it succeeds
    if config.ENABLE_HTML_FIRST_EXTRACTION:
        html = _fetch_html(url)
        if html:
            items = _extract_from_html_with_llm(html, url)
            if items:
                logger.info(
                    "HTML extraction extracted %s items from %s",
                    len(items),
                    url,
                )
                return items
        logger.info(
            "HTML extraction returned 0 items or fetch failed for %s, falling back to Apify",
            url,
        )

    # Apify browser actor fallback (for JS-heavy pages or when HTML path failed)
    if not config.APIFY_API_TOKEN:
        raise ScraperError(
            "APIFY_API_TOKEN is not set. Set it in .env to use Apify fallback for website scraping."
        )

    try:
        from apify_client import ApifyClient
    except ImportError:
        raise ScraperError(
            "apify-client is not installed. Run: pip install apify-client"
        )

    actor_id = actor_id or config.APIFY_AI_EXTRACTOR_ACTOR_ID
    timeout_secs = timeout_secs or config.APIFY_RUN_TIMEOUT_SECS

    if run_input is None:
        # Build instructions; optionally prepend wait-for-dynamic-content so the agent
        # waits for JS-rendered content (e.g. board/list cards) before extracting.
        instructions = config.APIFY_EXTRACTION_PROMPT
        if config.APIFY_WAIT_FOR_DYNAMIC_CONTENT:
            wait_secs = config.APIFY_PAGE_WAIT_SECS
            wait_instruction = (
                f"First wait for the page content to fully load: wait at least {wait_secs} seconds "
                "and ensure you see the main content (e.g. list of people, board members, cards, or directory). "
                "Then perform the following extraction: "
            )
            instructions = wait_instruction + instructions

        # Default input for AI Web Agent-style actors (startUrl + instructions + openaiApiKey required by apify/ai-web-agent)
        run_input = {
            "startUrl": url,
            "instructions": instructions,
            "prompt": instructions,
        }
        run_input["openaiApiKey"] = config.OPENAI_API_KEY
        # Some actors expect startUrls as list
        if "startUrl" in run_input and "startUrls" not in run_input:
            run_input["startUrls"] = [{"url": url}]

    client = ApifyClient(config.APIFY_API_TOKEN)
    logger.info(f"Running Apify actor {actor_id} for URL: {url}")

    try:
        run = client.actor(actor_id).call(
            run_input=run_input,
            timeout_secs=timeout_secs,
        )
    except Exception as e:
        logger.exception("Apify run failed")
        raise ScraperError(f"Apify run failed: {e}") from e

    if run is None:
        raise ScraperError("Apify run returned no result.")

    items: list[dict[str, Any]] = []
    default_kv_store_id = run.get("defaultKeyValueStoreId")
    if default_kv_store_id:
        try:
            record = client.key_value_store(
                default_kv_store_id).get_record("OUTPUT")
            if record is not None:
                payload = record.get("value")
                if isinstance(payload, list):
                    raw_items = payload
                elif isinstance(payload, dict):
                    raw_items = (
                        payload.get("result")
                        or payload.get("items")
                        or payload.get("people")
                        or []
                    )
                else:
                    raw_items = []
                if isinstance(raw_items, list):
                    items = [x for x in raw_items if isinstance(x, dict)]
        except Exception as e:
            logger.debug("Could not read key-value store OUTPUT: %s", e)

    if not items:
        default_dataset_id = run.get("defaultDatasetId")
        if default_dataset_id:
            try:
                items = list(client.dataset(
                    default_dataset_id).iterate_items())
            except Exception as e:
                logger.exception("Failed to read Apify dataset")
                raise ScraperError(
                    f"Failed to read scrape results: {e}") from e
        elif not default_kv_store_id:
            raise ScraperError(
                "Apify run produced no dataset or key-value store.")

    # Normalize to consistent shape
    normalized = []
    for item in items:
        if not isinstance(item, dict):
            continue
        norm = _normalize_item(item)
        if norm.get("firstName") or norm.get("lastName") or norm.get("organization"):
            normalized.append(norm)

    logger.info(f"Apify extracted {len(normalized)} items from {url}")
    return normalized


def scraped_items_to_truth_rows(
    items: list[dict[str, Any]],
    source_url: str = "",
) -> pd.DataFrame:
    """
    Map scraped items to Truth table columns (DataFrame).
    Does not add Email ID (unique) when absent so normalize does not drop rows.

    Args:
        items: List of normalized items (firstName, lastName, organization, email optional).
        source_url: Optional source URL for reference.

    Returns:
        DataFrame with BASE_COLUMNS; Lead Source = "Website Scrape".
        Rows with neither (First Name or Last Name) nor Company are dropped.
    """
    base_cols_no_sn = [
        c for c in BASE_COLUMNS
        if c not in ("S.N.", "UPDATE AS ON")
    ]
    # Exclude Email ID (unique) from initial set so we only add it when actor returns email
    base_cols_no_email = [
        c for c in base_cols_no_sn if c != "Email ID (unique)"]

    rows: list[dict[str, Any]] = []
    for item in items:
        first = (item.get("firstName") or "").strip()
        last = (item.get("lastName") or "").strip()
        org = (item.get("organization") or "").strip()
        email = (item.get("email") or "").strip()

        if not first and not last and not org:
            continue

        row: dict[str, Any] = {col: "" for col in base_cols_no_email}
        row["First Name"] = first
        row["Last Name"] = last
        row["Company Name (Based on Website Domain)"] = org
        row["Lead Source"] = "Website Scrape"
        row["Email Send (Yes/No)"] = "No"

        if email:
            row["Email ID (unique)"] = email

        rows.append(row)

    if not rows:
        df = pd.DataFrame(columns=base_cols_no_email)
    else:
        df = pd.DataFrame(rows)
        for col in base_cols_no_email:
            if col not in df.columns:
                df[col] = ""
        # Order columns: base_cols_no_sn order, include Email ID only if present
        ordered = [c for c in base_cols_no_sn if c in df.columns]
        df = df[ordered]

    if source_url and "Source URL" in base_cols_no_sn:
        df["Source URL"] = source_url
    logger.info(f"Mapped {len(df)} scraped rows to Truth columns")
    return df
