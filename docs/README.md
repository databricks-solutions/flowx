# flowx docs

Documentation site for [flowx](https://github.com/ghanse/flowx), built with [fumadocs](https://fumadocs.dev) and deployed to GitHub Pages.

## Local development

From the repo root:

```bash
make docs-install   # one-time: install bun deps
make docs-serve     # next dev — http://localhost:3000
```

## Build

```bash
make docs-build     # static export to docs/site
```

The `docs-release.yml` workflow runs the same build and publishes `docs/site` to GitHub Pages on every push to `main` that touches `docs/**`.

## Authoring

Pages live in `content/docs/` as `.mdx` files. The page order is controlled by `content/docs/meta.json`. Frontmatter fields:

```yaml
---
title: Page title
description: Short summary used as the page subtitle and meta description.
---
```

Components available out of the box: `Tabs` / `Tab`, `Callout`, `Steps` / `Step` &mdash; imported from `fumadocs-ui/components/...` at the top of each MDX file.
