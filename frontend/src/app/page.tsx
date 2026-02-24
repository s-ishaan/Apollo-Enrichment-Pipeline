import Link from "next/link";

export default function HomePage() {
  return (
    <div className="space-y-6">
      <p className="text-pretty text-gray-600">
        Upload Excel data, enrich with Apollo, scrape websites, or view and export your database.
      </p>
      <ul className="list-inside list-disc space-y-2 text-pretty">
        <li>
          <Link href="/upload-enrich" className="underline underline-offset-4">
            Upload & Enrich
          </Link>
          — Upload an Excel file and enrich contacts with Apollo.
        </li>
        <li>
          <Link href="/scrape-enrich" className="underline underline-offset-4">
            Scrape & Enrich
          </Link>
          — Extract contacts from a URL and enrich with Apollo.
        </li>
        <li>
          <Link href="/upload-base" className="underline underline-offset-4">
            Upload Base Data
          </Link>
          — Import Excel without enrichment.
        </li>
        <li>
          <Link href="/database" className="underline underline-offset-4">
            Database Viewer
          </Link>
          — Search, filter, and export records.
        </li>
      </ul>
    </div>
  );
}
