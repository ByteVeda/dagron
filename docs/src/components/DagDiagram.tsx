import React from 'react';
import Mermaid from '@theme/Mermaid';

interface DagDiagramProps {
  chart: string;
  caption?: string;
}

export default function DagDiagram({chart, caption}: DagDiagramProps) {
  return (
    <div className="dag-diagram">
      <Mermaid value={chart} />
      {caption && <p style={{fontSize: '0.85rem', color: 'var(--ifm-font-color-secondary)', marginTop: '0.5rem'}}>{caption}</p>}
    </div>
  );
}
