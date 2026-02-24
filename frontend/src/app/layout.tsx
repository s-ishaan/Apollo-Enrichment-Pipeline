import type { Metadata } from "next";
import Link from "next/link";
import "./globals.css";

export const metadata: Metadata = {
  title: "Apollo Enrichment Pipeline",
  description: "Upload, enrich, and manage contact data",
};

const navItems = [
  { href: "/upload-enrich", label: "Upload & Enrich" },
  { href: "/scrape-enrich", label: "Scrape & Enrich" },
  { href: "/upload-base", label: "Upload Base Data" },
  { href: "/database", label: "Database Viewer" },
];

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body className="min-h-dvh bg-gray-50 text-gray-900 antialiased">
        <div className="mx-auto max-w-7xl px-4 py-6">
          <header className="mb-8">
            <h1 className="text-2xl font-semibold text-balance tabular-nums">
              Apollo Enrichment Pipeline
            </h1>
            <nav className="mt-4 flex flex-wrap gap-4 border-b border-gray-200 pb-4" aria-label="Main">
              {navItems.map(({ href, label }) => (
                <Link
                  key={href}
                  href={href}
                  className="text-gray-600 underline-offset-4 hover:underline hover:text-gray-900"
                >
                  {label}
                </Link>
              ))}
            </nav>
          </header>
          <main>{children}</main>
        </div>
      </body>
    </html>
  );
}
