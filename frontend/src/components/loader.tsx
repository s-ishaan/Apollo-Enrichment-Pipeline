import { cn } from "@/lib/utils";

interface LoaderProps {
  /** Size of the spinner. */
  size?: "sm" | "md" | "lg";
  /** Optional class name for the wrapper. */
  className?: string;
  /** Accessible label for the loading indicator. */
  "aria-label"?: string;
}

const sizeClasses = {
  sm: "size-4 border-2",
  md: "size-6 border-2",
  lg: "size-8 border-2",
} as const;

/**
 * Basic loading spinner. Uses transform-only animation and respects
 * prefers-reduced-motion (stops spin, keeps visible).
 */
export function Loader({
  size = "md",
  className,
  "aria-label": ariaLabel = "Loading",
}: LoaderProps) {
  return (
    <div
      className={cn("flex items-center justify-center", className)}
      role="status"
      aria-label={ariaLabel}
      aria-busy="true"
    >
      <span
        className={cn(
          "inline-block rounded-full border-gray-300 border-t-gray-600",
          "animate-spin motion-reduce:animate-none",
          sizeClasses[size]
        )}
      />
    </div>
  );
}
