import type { BaseLayoutProps } from "fumadocs-ui/layouts/shared";
import { appName, gitConfig } from "./shared";

const PRIMARY_NAV_LINKS = [
  {
    text: "Guide",
    url: "/guide/getting-started",
  },
  {
    text: "API",
    url: "/api/core/core",
  },
  {
    text: "Typed & Reactive",
    url: "/typed-and-reactive",
  },
  {
    text: "Changelog",
    url: `https://github.com/${gitConfig.user}/${gitConfig.repo}/blob/${gitConfig.branch}/CHANGELOG.md`,
    external: true,
  },
];

export function baseOptions(): BaseLayoutProps {
  return {
    nav: {
      title: appName,
    },
    githubUrl: `https://github.com/${gitConfig.user}/${gitConfig.repo}`,
  };
}

export function homeOptions(): BaseLayoutProps {
  return {
    ...baseOptions(),
    links: PRIMARY_NAV_LINKS,
  };
}
