import React from 'react';
import Link from '@docusaurus/Link';

interface FeatureCardProps {
  title: string;
  description: string;
  guideLink?: string;
  apiLink?: string;
  icon?: string;
}

export default function FeatureCard({title, description, guideLink, apiLink, icon}: FeatureCardProps) {
  return (
    <div className="feature-card">
      <div className="feature-card__title">
        {icon && <span style={{marginRight: '0.5rem'}}>{icon}</span>}
        {title}
      </div>
      <div className="feature-card__description">{description}</div>
      <div className="feature-card__links">
        {guideLink && <Link to={guideLink}>Guide &rarr;</Link>}
        {apiLink && <Link to={apiLink}>API &rarr;</Link>}
      </div>
    </div>
  );
}
