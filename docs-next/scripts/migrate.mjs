#!/usr/bin/env node
/**
 * One-shot migrator: docs/pages/**.mdx → docs-next/content/docs/**.mdx
 *
 * Transforms applied to each MDX file:
 *  1. Drop frontmatter keys `sidebar_position` and `slug` (Fumadocs uses
 *     per-directory meta.json + file path for routing).
 *  2. Drop `import X from '@site/src/components/X'` lines — components
 *     are globally registered via src/components/mdx.tsx.
 *  3. Convert Docusaurus admonitions to Fumadocs `<Callout>`:
 *       :::tip Heading        →  <Callout type="info" title="Heading">
 *       … body                       … body
 *       :::                       </Callout>
 *     (note→note, warning→warn, danger→error, info→info, tip→info)
 *  4. Rename `intro.mdx` (Docusaurus's slug:/) to `index.mdx`
 *     (Fumadocs's root convention).
 *
 * Idempotent — re-running on already-migrated files is a no-op.
 */
import { mkdir, readdir, readFile, stat, writeFile } from "node:fs/promises";
import { dirname, join, relative } from "node:path";
import { fileURLToPath } from "node:url";

const HERE = dirname(fileURLToPath(import.meta.url));
const SRC = join(HERE, "..", "..", "docs", "pages");
const DST = join(HERE, "..", "content", "docs");

const ADMONITION_MAP = {
  tip: "info",
  note: "note",
  info: "info",
  warning: "warn",
  caution: "warn",
  danger: "error",
};

async function* walk(dir) {
  for (const entry of await readdir(dir, { withFileTypes: true })) {
    const full = join(dir, entry.name);
    if (entry.isDirectory()) {
      yield* walk(full);
    } else if (entry.isFile() && entry.name.endsWith(".mdx")) {
      yield full;
    }
  }
}

function transformFrontmatter(src) {
  const fmMatch = src.match(/^---\n([\s\S]*?)\n---\n?/);
  if (!fmMatch) return src;
  const body = fmMatch[1];
  const cleaned = body
    .split("\n")
    .filter((line) => !/^\s*(sidebar_position|slug)\s*:/.test(line))
    .join("\n");
  return `---\n${cleaned}\n---\n${src.slice(fmMatch[0].length)}`;
}

function dropSiteImports(src) {
  return (
    src
      .split("\n")
      .filter(
        (line) =>
          !/^import\s+\w+\s+from\s+['"]@site\/src\/components\/[^'"]+['"];?\s*$/.test(
            line.trim(),
          ),
      )
      .join("\n")
      // collapse 3+ blank lines that result from import removals
      .replace(/\n{3,}/g, "\n\n")
  );
}

/** Shiki's default bundle has no `dot` grammar — fall back to plain text. */
function relabelDotCodeBlocks(src) {
  return src.replace(/^```dot\s*$/gm, "```text");
}

function convertAdmonitions(src) {
  // Match :::<type> [optional title]\n…\n:::
  return src.replace(
    /^:::(tip|note|info|warning|caution|danger)([ \t]+([^\n]+))?\n([\s\S]*?)^:::\s*$/gm,
    (_full, type, _gap, title, body) => {
      const calloutType = ADMONITION_MAP[type] ?? "info";
      const trimmedBody = body.replace(/\s+$/g, "");
      const titleAttr = title ? ` title="${title.trim()}"` : "";
      return `<Callout type="${calloutType}"${titleAttr}>\n${trimmedBody}\n</Callout>`;
    },
  );
}

async function ensureDir(d) {
  await mkdir(d, { recursive: true });
}

async function migrateFile(srcPath) {
  const rel = relative(SRC, srcPath);
  // intro.mdx → index.mdx (Fumadocs root convention)
  const remapped = rel === "intro.mdx" ? "index.mdx" : rel;
  const dstPath = join(DST, remapped);
  await ensureDir(dirname(dstPath));

  let content = await readFile(srcPath, "utf8");
  content = transformFrontmatter(content);
  content = dropSiteImports(content);
  content = convertAdmonitions(content);
  content = relabelDotCodeBlocks(content);

  await writeFile(dstPath, content, "utf8");
  return { srcPath, dstPath, rel };
}

async function main() {
  await ensureDir(DST);

  // Sanity: source must exist
  try {
    const s = await stat(SRC);
    if (!s.isDirectory()) throw new Error("not a directory");
  } catch {
    console.error(`Source not found: ${SRC}`);
    process.exit(1);
  }

  let count = 0;
  for await (const srcPath of walk(SRC)) {
    const { rel } = await migrateFile(srcPath);
    process.stdout.write(`  ✓ ${rel}\n`);
    count += 1;
  }
  console.log(`\nMigrated ${count} MDX files → ${DST}`);
}

await main();
