import { cn } from "@/lib/cn";

export type Effect = "pure" | "read" | "write" | "network" | "nondeterministic";

const EFFECT_CLASSES: Record<Effect, string> = {
  pure: "bg-[var(--color-dagron-effect-pure)]",
  read: "bg-[var(--color-dagron-effect-read)]",
  write: "bg-[var(--color-dagron-effect-write)]",
  network: "bg-[var(--color-dagron-effect-network)]",
  nondeterministic: "bg-[var(--color-dagron-effect-nondeterministic)]",
};

const EFFECT_LABELS: Record<Effect, string> = {
  pure: "PURE",
  read: "READ",
  write: "WRITE",
  network: "NETWORK",
  nondeterministic: "ND",
};

export type EffectBadgeProps = {
  effect: Effect;
  label?: string;
  className?: string;
};

/**
 * Pill labelling a `@dagron.task`'s effect class — matches the runtime
 * `dagron.Effect` enum (PURE/READ/WRITE/NETWORK/NONDETERMINISTIC).
 */
export function EffectBadge({ effect, label, className }: EffectBadgeProps) {
  return (
    <span
      className={cn(
        "inline-block rounded-md px-2 py-0.5 text-[0.7rem] font-semibold uppercase tracking-wider text-white",
        EFFECT_CLASSES[effect],
        className,
      )}
    >
      {label ?? EFFECT_LABELS[effect]}
    </span>
  );
}
