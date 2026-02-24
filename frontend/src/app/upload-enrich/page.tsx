"use client";

import { useState } from "react";
import { enrichUpload, type EnrichUploadResult } from "@/lib/api";
import { cn } from "@/lib/utils";
import { EnrichmentBlockingOverlay } from "@/components/enrichment-blocking-overlay";

export default function UploadEnrichPage() {
  const [file, setFile] = useState<File | null>(null);
  const [enrichPeople, setEnrichPeople] = useState(true);
  const [enrichCompanies, setEnrichCompanies] = useState(true);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<EnrichUploadResult | null>(null);

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (!file) {
      setError("Please choose an Excel file.");
      return;
    }
    setError(null);
    setResult(null);
    setLoading(true);
    try {
      const data = await enrichUpload(file, enrichPeople, enrichCompanies);
      setResult(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Enrichment failed.");
    } finally {
      setLoading(false);
    }
  }

  return (
    <>
      {loading && <EnrichmentBlockingOverlay message="Enrichment in progress…" />}
      <div className="space-y-8" aria-hidden={loading}>
      <div>
        <h2 className="text-xl font-semibold text-balance">Upload & Enrich Data</h2>
        <p className="mt-2 text-pretty text-gray-600">
          Upload an Excel file (.xlsx) with organization and people data. The system will enrich
          using Apollo API and store in the database.
        </p>
      </div>

      <form onSubmit={handleSubmit} className="space-y-6">
        <div>
          <label htmlFor="file" className="block text-sm font-medium text-gray-700">
            Excel file
          </label>
          <input
            id="file"
            type="file"
            accept=".xlsx"
            className="mt-1 block w-full text-sm text-gray-600 file:mr-4 file:rounded file:border-0 file:bg-gray-100 file:px-4 file:py-2 file:text-sm file:font-medium"
            onChange={(e) => setFile(e.target.files?.[0] ?? null)}
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
            <span className="text-sm">Enrich people data (Apollo People API)</span>
          </label>
          <label className="flex items-center gap-2">
            <input
              type="checkbox"
              checked={enrichCompanies}
              onChange={(e) => setEnrichCompanies(e.target.checked)}
              className="size-4 rounded border-gray-300"
            />
            <span className="text-sm">Enrich company data (Apollo Organizations API)</span>
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
          {loading ? "Running…" : "Start enrichment"}
        </button>
      </form>

      {result && (
        <section className="space-y-4 rounded-lg border border-gray-200 bg-white p-6">
          <h3 className="font-medium text-balance">Results</h3>
          {result.empty_reason ? (
            <p className="text-pretty text-gray-600">
              No data rows to process. The file was empty, had only headers, or all rows were
              removed.
            </p>
          ) : (
            <>
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
              {result.warnings && result.warnings.length > 0 && (
                <ul className="list-inside list-disc text-sm text-amber-800">
                  {result.warnings.map((w, i) => (
                    <li key={i}>{w}</li>
                  ))}
                </ul>
              )}
            </>
          )}
        </section>
      )}
    </div>
    </>
  );
}
