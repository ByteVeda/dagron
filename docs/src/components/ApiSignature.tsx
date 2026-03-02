import React from 'react';
import CodeBlock from '@theme/CodeBlock';

interface ApiSignatureProps {
  name: string;
  signature: string;
  language?: string;
}

export default function ApiSignature({name, signature, language = 'python'}: ApiSignatureProps) {
  const id = name.toLowerCase().replace(/[^a-z0-9]+/g, '-');
  return (
    <div className="api-signature" id={id}>
      <a className="api-signature__anchor" href={`#${id}`}>
        {name}
      </a>
      <CodeBlock language={language}>{signature}</CodeBlock>
    </div>
  );
}
