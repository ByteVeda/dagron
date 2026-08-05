import { cn } from "@/lib/cn";

export type Status =
  | "pending"
  | "running"
  | "completed"
  | "failed"
  | "skipped"
  | "timed-out"
  | "cancelled"
  | "cache-hit";

const STATUS_CLASSES: Record<Status, string> = {
  pending: "bg-[var(--color-dagron-pending)]",
  running: "bg-[var(--color-dagron-running)]",
  completed: "bg-[var(--color-dagron-completed)]",
  failed: "bg-[var(--color-dagron-failed)]",
  skipped: "bg-[var(--color-dagron-skipped)]",
  "timed-out": "bg-[var(--color-dagron-timed-out)]",
  cancelled: "bg-[var(--color-dagron-cancelled)]",
  "cache-hit": "bg-[var(--color-dagron-cache-hit)]",
};

export type StatusBadgeProps = {
  status: Status;
  label?: string;
  className?: string;
};

/**
 * Inline status pill used to annotate node states in execution traces and
 * conceptual diagrams (running / completed / failed / skipped / …).
 */
export function StatusBadge({ status, label, className }: StatusBadgeProps) {
  return (
    <span
      className={cn(
        "inline-block rounded-full px-2 py-0.5 text-[0.7rem] font-semibold uppercase tracking-wider text-white",
        STATUS_CLASSES[status],
        className,
      )}
    >
      {label ?? status.replace("-", " ")}
    </span>
  );
}
