# DBSOF UI

This monorepo packages a reusable studio-style UI that can be wired up to any
infrastructure backend. The codebase keeps the full component library intact
while moving provider-specific integrations behind a platform abstraction so it
can act as a starting point for new projects. Out of the box it runs purely as
frontend UI; you can later add your own API adapter without changing the UI
components.

## Workspaces

This repo is organised as follows:

- `/frontend`: a reusable studio-style UI that can be wired up to any
infrastructure backend. The codebase keeps the full component library intact
while moving provider-specific integrations behind a platform abstraction so it
can act as a starting point for new projects. Out of the box it runs purely as
frontend UI; you can later add your own API adapter without changing the UI
components. See <./frontend/readme.md> for build and development steps.
- `/server`: Mocked back-end server for now.

## Getting started

Start the front-end (at `http://localhost:3002/ui`):

```sh
cd frontend
yarn install
yarn dev
```

Start the back-end (at )`http://localhost:5757`:

```sh
cd server
uv run fastapi run main.py --port 5757
```
