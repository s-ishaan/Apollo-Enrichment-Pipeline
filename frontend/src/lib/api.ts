/**
 * API client for Apollo Enrichment backend.
 * Uses NEXT_PUBLIC_API_URL (e.g. http://localhost:8000).
 */

const getBaseUrl = () =>
  typeof process.env.NEXT_PUBLIC_API_URL === "string" &&
  process.env.NEXT_PUBLIC_API_URL
    ? process.env.NEXT_PUBLIC_API_URL.replace(/\/$/, "")
    : "http://localhost:8000";

export interface EnrichUploadResult {
  total_processed: number;
  new_inserts: number;
  updates: number;
  failed: number;
  people_enriched?: number;
  orgs_enriched?: number;
  errors?: Array<{ email: string; message: string }>;
  warnings?: string[];
  empty_reason?: string | null;
  skipped_no_email?: number;
  org_enrichment_skipped_no_domain?: number;
  inserted_emails?: string[];
  updated_emails?: string[];
  failed_records?: Array<{ email: string; error: string }>;
}

export interface ScrapeResult {
  total_processed: number;
  new_inserts: number;
  updates: number;
  failed: number;
  people_enriched?: number;
  orgs_enriched?: number;
  errors?: Array<{ email: string; message: string }>;
  skipped_no_email?: number;
  saved_records?: Array<Record<string, string>>;
  skipped_no_email_records?: Array<Record<string, string>>;
}

export interface BaseUploadResult {
  stats: { inserted: number; updated: number; failed: number };
  total: number;
}

export interface DbStats {
  total_records: number;
  total_columns: number;
  recent_updates_7_days: number;
  by_lead_source?: Record<string, number>;
}

export interface DbColumns {
  base: string[];
  apollo: string[];
}

export interface DbRecordsResponse {
  records: Record<string, unknown>[];
  total: number;
}

export type FilterParams = {
  limit?: number;
  offset?: number;
  email?: string;
  company?: string;
  country?: string;
  first_name?: string;
  last_name?: string;
  job_title?: string;
  industry?: string;
  state?: string;
  website?: string;
  lead_source?: string;
  client_type?: string;
  email_send?: string;
};

async function handleResponse<T>(res: Response): Promise<T> {
  if (!res.ok) {
    const text = await res.text();
    let detail = text;
    try {
      const j = JSON.parse(text) as { detail?: string };
      if (typeof j.detail === "string") detail = j.detail;
    } catch {
      // use text as-is
    }
    throw new Error(detail || `Request failed: ${res.status}`);
  }
  return res.json() as Promise<T>;
}

export async function enrichUpload(
  file: File,
  enrichPeople: boolean,
  enrichCompanies: boolean
): Promise<EnrichUploadResult> {
  const form = new FormData();
  form.append("file", file);
  form.append("enrich_people", String(enrichPeople));
  form.append("enrich_companies", String(enrichCompanies));
  const res = await fetch(`${getBaseUrl()}/enrich/upload`, {
    method: "POST",
    body: form,
  });
  return handleResponse<EnrichUploadResult>(res);
}

export async function enrichScrape(
  url: string,
  enrichPeople: boolean,
  enrichCompanies: boolean
): Promise<ScrapeResult> {
  const res = await fetch(`${getBaseUrl()}/enrich/scrape`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      url,
      enrich_people: enrichPeople,
      enrich_companies: enrichCompanies,
    }),
  });
  return handleResponse<ScrapeResult>(res);
}

export async function uploadBase(file: File): Promise<BaseUploadResult> {
  const form = new FormData();
  form.append("file", file);
  const res = await fetch(`${getBaseUrl()}/upload/base`, {
    method: "POST",
    body: form,
  });
  return handleResponse<BaseUploadResult>(res);
}

export async function getDbStats(): Promise<DbStats> {
  const res = await fetch(`${getBaseUrl()}/db/stats`);
  return handleResponse<DbStats>(res);
}

export async function getDbColumns(): Promise<DbColumns> {
  const res = await fetch(`${getBaseUrl()}/db/columns`);
  return handleResponse<DbColumns>(res);
}

export async function getDbRecords(params: FilterParams): Promise<DbRecordsResponse> {
  const u = new URL(`${getBaseUrl()}/db/records`);
  Object.entries(params).forEach(([k, v]) => {
    if (v !== undefined && v !== "") u.searchParams.set(k, String(v));
  });
  const res = await fetch(u.toString());
  return handleResponse<DbRecordsResponse>(res);
}

/**
 * Fetch export as blob and trigger download using Content-Disposition filename or default.
 */
export async function downloadExport(params: Omit<FilterParams, "limit" | "offset">): Promise<void> {
  const u = new URL(`${getBaseUrl()}/db/export`);
  Object.entries(params).forEach(([k, v]) => {
    if (v !== undefined && v !== "") u.searchParams.set(k, String(v));
  });
  const res = await fetch(u.toString());
  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || `Export failed: ${res.status}`);
  }
  const disposition = res.headers.get("Content-Disposition");
  const match = disposition && /filename="?([^";]+)"?/.exec(disposition);
  const filename = match ? match[1].trim() : "apollo_export.xlsx";
  const blob = await res.blob();
  const a = document.createElement("a");
  a.href = URL.createObjectURL(blob);
  a.download = filename;
  a.click();
  URL.revokeObjectURL(a.href);
}
