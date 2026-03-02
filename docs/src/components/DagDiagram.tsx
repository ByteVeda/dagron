import React, {useMemo} from 'react';
import Mermaid from '@theme/Mermaid';
import {useColorMode} from '@docusaurus/theme-common';

/**
 * Maps light-mode hex colors (used in classDef/style directives) to dark-mode equivalents.
 * Fills go darker/more saturated; strokes go lighter/brighter for contrast on dark backgrounds.
 * Keys must be lowercase.
 */
const DARK_COLOR_MAP: Record<string, string> = {
  // Green (success/active) — fills
  '#c8e6c9': '#1b5e20',
  '#d4edda': '#1b5e20',
  '#e8f5e9': '#1b5e20',
  // Green — strokes
  '#2e7d32': '#66bb6a',
  '#28a745': '#66bb6a',

  // Blue (processing) — fill / stroke
  '#e3f2fd': '#0d47a1',
  '#1565c0': '#64b5f6',

  // Blue (ancestor) — fill / stroke
  '#dbeafe': '#1e3a5f',
  '#3b82f6': '#90caf9',

  // Indigo (reused/restored) — fill / stroke
  '#e0e7ff': '#283593',
  '#6366f1': '#9fa8da',

  // Red (dirty/error/critical) — fills
  '#fecaca': '#7f1d1d',
  '#ffcdd2': '#7f1d1d',
  '#fee2e2': '#7f1d1d',
  // Red — strokes
  '#ef4444': '#ef5350',
  '#c62828': '#ef5350',

  // Orange (recomputed/warning) — fills
  '#fed7aa': '#7c2d12',
  '#fff3e0': '#7c2d12',
  '#ffcc80': '#7c2d12',
  // Orange — strokes
  '#f97316': '#fb8c00',
  '#e65100': '#fb8c00',
  '#ef6c00': '#ff9800',

  // Yellow (cached/leaf) — fills
  '#fff9c4': '#5f370e',
  '#fff3cd': '#5f370e',
  // Yellow — stroke
  '#ffc107': '#fdd835',

  // Amber — fill / stroke
  '#f9a825': '#f57f17',
  '#f57f17': '#ffb300',

  // Lime (cutoff) — fill / stroke
  '#d9f99d': '#365314',
  '#65a30d': '#9ccc65',

  // Gray (skipped/default) — fills
  '#e0e0e0': '#37474f',
  '#e2e8f0': '#37474f',
  // Gray — strokes
  '#9e9e9e': '#b0bec5',
  '#94a3b8': '#b0bec5',
};

const COLOR_PATTERN = new RegExp(
  Object.keys(DARK_COLOR_MAP).join('|'),
  'gi',
);

interface DagDiagramProps {
  chart: string;
  caption?: string;
}

export default function DagDiagram({chart, caption}: DagDiagramProps) {
  const {colorMode} = useColorMode();

  const value = useMemo(() => {
    if (colorMode !== 'dark') return chart;
    return chart.replace(
      COLOR_PATTERN,
      (match) => DARK_COLOR_MAP[match.toLowerCase()] ?? match,
    );
  }, [chart, colorMode]);

  return (
    <div className="dag-diagram">
      <Mermaid value={value} />
      {caption && <p style={{fontSize: '0.85rem', color: 'var(--ifm-font-color-secondary)', marginTop: '0.5rem'}}>{caption}</p>}
    </div>
  );
}
