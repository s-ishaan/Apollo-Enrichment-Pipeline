"use client";

import { useState, useEffect, useCallback } from "react";
import {
  getDbStats,
  getDbColumns,
  getDbRecords,
  downloadExport,
  type DbStats,
  type DbColumns,
  type FilterParams,
} from "@/lib/api";

const PAGE_SIZES = [25, 50, 100, 500];
const FILTER_KEYS: { key: keyof FilterParams; label: string }[] = [
  { key: "email", label: "Email" },
  { key: "company", label: "Company" },
  { key: "country", label: "Country" },
  { key: "first_name", label: "First name" },
  { key: "last_name", label: "Last name" },
  { key: "job_title", label: "Job title" },
  { key: "industry", label: "Industry" },
  { key: "state", label: "State" },
  { key: "website", label: "Website" },
  { key: "lead_source", label: "Lead source" },
  { key: "client_type", label: "Client type" },
  { key: "email_send", label: "Email send" },
];

export default function DatabasePage() {
  const [stats, setStats] = useState<DbStats | null>(null);
  const [columns, setColumns] = useState<DbColumns | null>(null);
  const [records, setRecords] = useState<Record<string, unknown>[]>([]);
  const [total, setTotal] = useState(0);
  const [loadingStats, setLoadingStats] = useState(true);
  const [loadingRecords, setLoadingRecords] = useState(true);
  const [filters, setFilters] = useState<FilterParams>({
    limit: 50,
    offset: 0,
  });
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(50);
  const [exporting, setExporting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [columnsOpen, setColumnsOpen] = useState(false);

  const loadStats = useCallback(async () => {
    setLoadingStats(true);
    setError(null);
    try {
      const data = await getDbStats();
      setStats(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load stats.");
    } finally {
      setLoadingStats(false);
    }
  }, []);

  const loadColumns = useCallback(async () => {
    try {
      const data = await getDbColumns();
      setColumns(data);
    } catch {
      // non-blocking
    }
  }, []);

  const loadRecords = useCallback(async () => {
    setLoadingRecords(true);
    setError(null);
    try {
      const params: FilterParams = {
        ...filters,
        limit: pageSize,
        offset: (page - 1) * pageSize,
      };
      const data = await getDbRecords(params);
      setRecords(data.records);
      setTotal(data.total);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load records.");
    } finally {
      setLoadingRecords(false);
    }
  }, [filters, page, pageSize]);

  useEffect(() => {
    loadStats();
    loadColumns();
  }, [loadStats, loadColumns]);

  useEffect(() => {
    loadRecords();
  }, [loadRecords]);

  function handleFilterChange(key: keyof FilterParams, value: string | number | undefined) {
    setFilters((prev) => ({ ...prev, [key]: value || undefined }));
    setPage(1);
  }

  function handleApplyFilters() {
    loadRecords();
  }

  async function handleExport() {
    setExporting(true);
    setError(null);
    try {
      const { limit: _l, offset: _o, ...exportParams } = filters;
      await downloadExport(exportParams);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Export failed.");
    } finally {
      setExporting(false);
    }
  }

  const maxPage = Math.max(1, Math.ceil(total / pageSize));
  const orderedCols = records.length
    ? (Object.keys(records[0]) as string[]).sort((a, b) => {
        const aBase = !a.startsWith("Apollo");
        const bBase = !b.startsWith("Apollo");
        if (aBase && !bBase) return -1;
        if (!aBase && bBase) return 1;
        return a.localeCompare(b);
      })
    : [];

  return (
    <div className="space-y-8">
      <div>
        <h2 className="text-xl font-semibold text-balance">Database Viewer</h2>
        <p className="mt-2 text-pretty text-gray-600">
          Search, filter, and export records. Use the filters below and apply to load data.
        </p>
      </div>

      {error && (
        <div className="rounded-md bg-red-50 p-4 text-sm text-red-800" role="alert">
          {error}
        </div>
      )}

      {/* Stats */}
      <section>
        <h3 className="mb-3 text-sm font-medium text-gray-700">Statistics</h3>
        {loadingStats ? (
          <div className="h-20 animate-pulse rounded-lg bg-gray-200" />
        ) : stats ? (
          <div className="grid grid-cols-3 gap-4">
            <div className="rounded-lg border border-gray-200 bg-white p-4">
              <p className="text-sm text-gray-500">Total records</p>
              <p className="tabular-nums font-medium">{stats.total_records}</p>
            </div>
            <div className="rounded-lg border border-gray-200 bg-white p-4">
              <p className="text-sm text-gray-500">Total columns</p>
              <p className="tabular-nums font-medium">{stats.total_columns}</p>
            </div>
            <div className="rounded-lg border border-gray-200 bg-white p-4">
              <p className="text-sm text-gray-500">Recent updates (7 days)</p>
              <p className="tabular-nums font-medium">{stats.recent_updates_7_days}</p>
            </div>
          </div>
        ) : null}
      </section>

      {/* Filters */}
      <section className="rounded-lg border border-gray-200 bg-white p-4">
        <h3 className="mb-3 text-sm font-medium text-gray-700">Search & filter</h3>
        <div className="grid grid-cols-2 gap-4 sm:grid-cols-3 md:grid-cols-4">
          {FILTER_KEYS.map(({ key, label }) => (
            <div key={key}>
              <label htmlFor={`filter-${key}`} className="block text-xs text-gray-500">
                {label}
              </label>
              <input
                id={`filter-${key}`}
                type="text"
                value={(filters[key] as string) ?? ""}
                onChange={(e) => handleFilterChange(key, e.target.value)}
                placeholder={`Search by ${label.toLowerCase()}…`}
                className="mt-0.5 w-full rounded border border-gray-300 px-2 py-1.5 text-sm focus:border-gray-500 focus:outline-none focus:ring-1 focus:ring-gray-500"
              />
            </div>
          ))}
        </div>
        <button
          type="button"
          onClick={handleApplyFilters}
          className="mt-4 rounded-md bg-gray-900 px-4 py-2 text-sm font-medium text-white hover:bg-gray-800"
        >
          Apply filters
        </button>
      </section>

      {/* Pagination */}
      <div className="flex flex-wrap items-center gap-4">
        <label className="flex items-center gap-2">
          <span className="text-sm text-gray-600">Records per page</span>
          <select
            value={pageSize}
            onChange={(e) => {
              setPageSize(Number(e.target.value));
              setPage(1);
            }}
            className="rounded border border-gray-300 px-2 py-1 text-sm"
          >
            {PAGE_SIZES.map((n) => (
              <option key={n} value={n}>
                {n}
              </option>
            ))}
          </select>
        </label>
        <span className="text-sm text-gray-600">
          Total matching: <span className="tabular-nums font-medium">{total}</span>
        </span>
        <div className="flex items-center gap-2">
          <button
            type="button"
            onClick={() => setPage((p) => Math.max(1, p - 1))}
            disabled={page <= 1}
            className="rounded border border-gray-300 px-2 py-1 text-sm disabled:opacity-50"
          >
            Previous
          </button>
          <span className="text-sm tabular-nums">
            Page {page} of {maxPage}
          </span>
          <button
            type="button"
            onClick={() => setPage((p) => Math.min(maxPage, p + 1))}
            disabled={page >= maxPage}
            className="rounded border border-gray-300 px-2 py-1 text-sm disabled:opacity-50"
          >
            Next
          </button>
        </div>
      </div>

      {/* Table */}
      <section className="overflow-x-auto rounded-lg border border-gray-200 bg-white">
        {loadingRecords ? (
          <div className="h-96 animate-pulse bg-gray-100" />
        ) : records.length === 0 ? (
          <p className="p-8 text-center text-gray-500">No records found.</p>
        ) : (
          <table className="min-w-full text-left text-sm">
            <thead>
              <tr className="border-b border-gray-200 bg-gray-50">
                {orderedCols.map((col) => (
                  <th key={col} className="whitespace-nowrap px-4 py-2 font-medium text-gray-700">
                    {col}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {records.map((row, i) => (
                <tr key={i} className="border-b border-gray-100">
                  {orderedCols.map((col) => (
                    <td key={col} className="max-w-xs truncate px-4 py-2 tabular-nums">
                      {row[col] != null ? String(row[col]) : ""}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </section>

      {/* Export */}
      <div>
        <button
          type="button"
          onClick={handleExport}
          disabled={exporting || total === 0}
          className="rounded-md bg-gray-900 px-4 py-2 text-sm font-medium text-white hover:bg-gray-800 disabled:opacity-50"
        >
          {exporting ? "Preparing…" : "Export to Excel"}
        </button>
      </div>

      {/* Column info */}
      <section>
        <button
          type="button"
          onClick={() => setColumnsOpen((o) => !o)}
          className="text-sm font-medium text-gray-700 underline-offset-4 hover:underline"
        >
          {columnsOpen ? "Hide" : "Show"} column information
        </button>
        {columnsOpen && columns && (
          <div className="mt-2 grid gap-4 sm:grid-cols-2">
            <div className="rounded border border-gray-200 p-4">
              <p className="text-sm font-medium text-gray-700">Base columns ({columns.base.length})</p>
              <ul className="mt-2 max-h-48 list-inside list-disc overflow-y-auto text-xs text-gray-600">
                {columns.base.map((c) => (
                  <li key={c}>{c}</li>
                ))}
              </ul>
            </div>
            <div className="rounded border border-gray-200 p-4">
              <p className="text-sm font-medium text-gray-700">Apollo columns ({columns.apollo.length})</p>
              <ul className="mt-2 max-h-48 list-inside list-disc overflow-y-auto text-xs text-gray-600">
                {columns.apollo.length ? columns.apollo.map((c) => <li key={c}>{c}</li>) : <li>None yet</li>}
              </ul>
            </div>
          </div>
        )}
      </section>
    </div>
  );
}
