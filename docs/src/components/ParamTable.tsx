import React from 'react';

interface Param {
  name: string;
  type: string;
  default?: string;
  description: string;
}

interface ParamTableProps {
  params: Param[];
}

export default function ParamTable({params}: ParamTableProps) {
  return (
    <table className="param-table">
      <thead>
        <tr>
          <th>Parameter</th>
          <th>Type</th>
          <th>Default</th>
          <th>Description</th>
        </tr>
      </thead>
      <tbody>
        {params.map((p) => (
          <tr key={p.name}>
            <td><code>{p.name}</code></td>
            <td><code>{p.type}</code></td>
            <td>{p.default ? <code>{p.default}</code> : <em>required</em>}</td>
            <td>{p.description}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
