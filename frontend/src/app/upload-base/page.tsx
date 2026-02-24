"use client";

import { useState } from "react";
import { uploadBase, type BaseUploadResult } from "@/lib/api";
import { cn } from "@/lib/utils";

export default function UploadBasePage() {
  const [file, setFile] = useState<File | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<BaseUploadResult | null>(null);

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
      const data = await uploadBase(file);
      setResult(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Upload failed.");
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="space-y-8">
      <div>
        <h2 className="text-xl font-semibold text-balance">Upload Base Data</h2>
        <p className="mt-2 text-pretty text-gray-600">
          Upload Excel data directly to the database without Apollo enrichment. Use this for
          importing existing contact data or bulk uploads.
        </p>
      </div>

      <form onSubmit={handleSubmit} className="space-y-6">
        <div>
          <label htmlFor="file" className="block text-sm font-medium text-gray-700">
            Excel file (.xlsx)
          </label>
          <input
            id="file"
            type="file"
            accept=".xlsx"
            className="mt-1 block w-full text-sm text-gray-600 file:mr-4 file:rounded file:border-0 file:bg-gray-100 file:px-4 file:py-2 file:text-sm file:font-medium"
            onChange={(e) => setFile(e.target.files?.[0] ?? null)}
          />
        </div>

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
          {loading ? "Uploading…" : "Upload to database"}
        </button>
      </form>

      {result && (
        <section className="space-y-4 rounded-lg border border-gray-200 bg-white p-6">
          <h3 className="font-medium text-balance">Results</h3>
          <div className="grid grid-cols-3 gap-4">
            <div>
              <p className="text-sm text-gray-500">Total processed</p>
              <p className="tabular-nums">{result.total}</p>
            </div>
            <div>
              <p className="text-sm text-gray-500">New records</p>
              <p className="tabular-nums">{result.stats.inserted}</p>
            </div>
            <div>
              <p className="text-sm text-gray-500">Updated records</p>
              <p className="tabular-nums">{result.stats.updated}</p>
            </div>
          </div>
          {result.stats.failed > 0 && (
            <p className="text-sm text-amber-800">{result.stats.failed} records failed to upload.</p>
          )}
        </section>
      )}
    </div>
  );
}
