# API Documentation — Local Development

## Live Site

API docs are published to Kryptonite on push to `master`, `beta`, and `staging`:

- https://k.atlan.dev/atlas-metastore/master
- https://k.atlan.dev/atlas-metastore/beta
- https://k.atlan.dev/atlas-metastore/staging

## Prerequisites

- Python 3.10+

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install mkdocs-material
```

## Local Preview

```bash
mkdocs serve
```

Open http://localhost:8000 in your browser. The site auto-reloads on file changes.

## Build Static Site

```bash
mkdocs build
```

Output is written to `site/` (gitignored).

## Deployment

The site auto-deploys to Kryptonite (S3) via `.github/workflows/docs.yml` on push to `master`/`beta`/`staging` when files under `docs/` or `mkdocs.yml` change. You can also trigger it manually from the Actions tab.

## Adding New API Docs

1. Create a new markdown file under `docs/api/` (e.g. `docs/api/entity-guid.md`)
2. Add it to the `nav` section in `mkdocs.yml`
3. Preview locally, then push
