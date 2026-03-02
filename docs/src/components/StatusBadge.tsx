import React from 'react';

type Status = 'pending' | 'running' | 'completed' | 'failed' | 'skipped' | 'timed-out' | 'cancelled' | 'cache-hit';

interface StatusBadgeProps {
  status: Status;
  label?: string;
}

export default function StatusBadge({status, label}: StatusBadgeProps) {
  return (
    <span className={`status-badge status-badge--${status}`}>
      {label ?? status}
    </span>
  );
}
