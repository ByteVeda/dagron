import Link from "next/link";
import type { ReactNode } from "react";
import { cn } from "@/lib/cn";

export type FeatureCardProps = {
  title: string;
  description: string;
  icon?: ReactNode;
  guideHref?: string;
  apiHref?: string;
  className?: string;
};

/**
 * Card with a title, short description, and optional Guide/API links.
 *
 * Used both on the home page (in a grid) and inside MDX pages for cross-linking
 * between sections of the documentation.
 */
export function FeatureCard({
  title,
  description,
  icon,
  guideHref,
  apiHref,
  className,
}: FeatureCardProps) {
  return (
    <div
      className={cn(
        "flex h-full flex-col rounded-lg border border-fd-border bg-fd-card p-5",
        "transition-all duration-200 hover:border-fd-primary hover:shadow-md",
        className,
      )}
    >
      <div className="flex items-start gap-3 mb-2">
        {icon ? <div className="text-2xl leading-none">{icon}</div> : null}
        <h3 className="text-base font-semibold text-fd-foreground">{title}</h3>
      </div>
      <p className="text-sm text-fd-muted-foreground mb-2 leading-relaxed">
        {description}
      </p>
      {(guideHref || apiHref) && (
        <div className="flex items-center gap-4 text-sm">
          {guideHref ? (
            <Link
              href={guideHref}
              className="text-fd-primary hover:underline font-medium"
            >
              Guide →
            </Link>
          ) : null}
          {apiHref ? (
            <Link
              href={apiHref}
              className="text-fd-primary hover:underline font-medium"
            >
              API →
            </Link>
          ) : null}
        </div>
      )}
    </div>
  );
}

/** Convenience wrapper that lays out a responsive grid of FeatureCards. */
export function FeatureGrid({
  children,
  className,
}: {
  children: ReactNode;
  className?: string;
}) {
  return (
    <div
      className={cn(
        "grid gap-4 my-6",
        "grid-cols-1 sm:grid-cols-2 lg:grid-cols-3",
        className,
      )}
    >
      {children}
    </div>
  );
}
