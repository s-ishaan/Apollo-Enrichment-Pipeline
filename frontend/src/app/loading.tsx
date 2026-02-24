import { Loader } from "@/components/loader";

/**
 * Root loading UI shown during route transitions (Suspense fallback).
 */
export default function Loading() {
  return (
    <div className="flex min-h-[12rem] items-center justify-center">
      <Loader size="lg" aria-label="Loading page" />
    </div>
  );
}
