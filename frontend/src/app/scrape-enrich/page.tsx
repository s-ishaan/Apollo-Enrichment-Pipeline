"use client";

import { useState } from "react";
import { enrichScrape, type ScrapeResult } from "@/lib/api";
import { cn } from "@/lib/utils";
import { EnrichmentBlockingOverlay } from "@/components/enrichment-blocking-overlay";

export default function ScrapeEnrichPage() {
  const [url, setUrl] = useState("");
  const [enrichPeople, setEnrichPeople] = useState(true);
  const [enrichCompanies, setEnrichCompanies] = useState(true);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<ScrapeResult | null>(null);

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (!url.trim()) {
      setError("Please enter a URL.");
      return;
    }
    setError(null);
    setResult(null);
    setLoading(true);
    try {
      const data = await enrichScrape(url.trim(), enrichPeople, enrichCompanies);
      setResult(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Scrape failed.");
    } finally {
      setLoading(false);
    }
  }

  return (
    <>
      {loading && <EnrichmentBlockingOverlay message="Scraping & enrichment in progress…" />}
      <div className="space-y-8" aria-hidden={loading}>
        <div>
          <h2 className="text-xl font-semibold text-balance">Scrape & Enrich</h2>
          <p className="mt-2 text-pretty text-gray-600">
            Enter a URL to extract person names and organizations with an AI extractor, then enrich
            via Apollo and save to the database (only rows with an email are stored).
          </p>
        </div>

        <form onSubmit={handleSubmit} className="space-y-6">
          <div>
            <label htmlFor="url" className="block text-sm font-medium text-gray-700">
              Page URL
            </label>
            <input
              id="url"
              type="url"
              placeholder="https://example.com/team"
              value={url}
              onChange={(e) => setUrl(e.target.value)}
              className="mt-1 block w-full rounded-md border border-gray-300 px-3 py-2 text-sm shadow-sm focus:border-gray-500 focus:outline-none focus:ring-1 focus:ring-gray-500"
            />
          </div>

          <fieldset className="space-y-3">
            <legend className="text-sm font-medium text-gray-700">Enrichment options</legend>
            <label className="flex items-center gap-2">
              <input
                type="checkbox"
                checked={enrichPeople}
                onChange={(e) => setEnrichPeople(e.target.checked)}
                className="size-4 rounded border-gray-300"
              />
              <span className="text-sm">Enrich people data</span>
            </label>
            <label className="flex items-center gap-2">
              <input
                type="checkbox"
                checked={enrichCompanies}
                onChange={(e) => setEnrichCompanies(e.target.checked)}
                className="size-4 rounded border-gray-300"
              />
              <span className="text-sm">Enrich company data</span>
            </label>
          </fieldset>

          {error && (
            <div className="rounded-md bg-red-50 p-4 text-sm text-red-800" role="alert">
              {error}
            </div>
          )}

          <button
            type="submit"
            disabled={loading}
            className={cn(
              "rounded-md bg-gray-900 px-4 py-2 text-sm font-medium text-white hover:bg-gray-800 disabled:opacity-50",
            )}
          >
            {loading ? "Running…" : "Run scrape & enrich"}
          </button>
        </form>

        {result && (
          <section className="space-y-4 rounded-lg border border-gray-200 bg-white p-6">
            <h3 className="font-medium text-balance">Results</h3>
            <div className="grid grid-cols-2 gap-4 sm:grid-cols-4">
              <div>
                <p className="text-sm text-gray-500">Total processed</p>
                <p className="tabular-nums">{result.total_processed}</p>
              </div>
              <div>
                <p className="text-sm text-gray-500">New records</p>
                <p className="tabular-nums">{result.new_inserts}</p>
              </div>
              <div>
                <p className="text-sm text-gray-500">Updated records</p>
                <p className="tabular-nums">{result.updates}</p>
              </div>
              <div>
                <p className="text-sm text-gray-500">Failed</p>
                <p className="tabular-nums">{result.failed}</p>
              </div>
            </div>
            {(enrichPeople || enrichCompanies) && (
              <div className="grid grid-cols-2 gap-4 sm:grid-cols-4">
                <div>
                  <p className="text-sm text-gray-500">People enriched</p>
                  <p className="tabular-nums">{result.people_enriched ?? 0}</p>
                </div>
                <div>
                  <p className="text-sm text-gray-500">Companies enriched</p>
                  <p className="tabular-nums">{result.orgs_enriched ?? 0}</p>
                </div>
              </div>
            )}
            {result.errors && result.errors.length > 0 && (
              <div>
                <p className="text-sm font-medium text-gray-700">Errors</p>
                <ul className="mt-1 list-inside list-disc text-sm text-red-800">
                  {result.errors.map((err, i) => (
                    <li key={i}>
                      {err.email}: {err.message}
                    </li>
                  ))}
                </ul>
              </div>
            )}
          </section>
        )}
      </div>
    </>
  );
}
