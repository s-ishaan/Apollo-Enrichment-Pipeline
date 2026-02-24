"use client";

/**
 * Full-page overlay shown during enrichment. Blocks all pointer and keyboard
 * interaction with the page until enrichment completes.
 */
export function EnrichmentBlockingOverlay({
  message = "Enrichment in progress…",
}: {
  message?: string;
}) {
  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-gray-900/50"
      aria-busy="true"
      aria-live="polite"
      role="status"
      style={{ touchAction: "none" }}
    >
      <div className="rounded-lg border border-gray-200 bg-white px-6 py-4 shadow-lg">
        <p className="text-pretty font-medium text-gray-900">{message}</p>
        <p className="mt-1 text-sm text-gray-600">Please wait. Do not navigate away.</p>
      </div>
    </div>
  );
}
