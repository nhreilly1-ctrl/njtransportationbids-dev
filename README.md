# NJ Transportation Bid Registry

This repository contains the FastAPI app for the New Jersey transportation bid registry and is set up for deployment on Render with a PostgreSQL database.

## What is in this repo

- `app/main.py` FastAPI application
- `render.yaml` Render blueprint
- `Dockerfile` production container build
- `requirements.txt` Python dependencies
- `docs/LAUNCH_CHECKLIST.md` shortest path to make the site public

## Local run

```bash
cp .env.example .env
docker compose up --build
```

If you are not using Docker locally:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload
```

## Render deploy

1. Push this folder to GitHub.
2. Create a new Render web service from the repo.
3. Create a Render PostgreSQL database.
4. Set `DATABASE_URL`, `ADMIN_USERNAME`, and `ADMIN_PASSWORD`.
5. Deploy and verify `/health`.
6. Add the custom domain in Render.
7. Point Cloudflare DNS at the Render hostname.

Use the detailed guide in `docs/LAUNCH_CHECKLIST.md`.

## Notes

The app creates its database tables on startup and exposes `/health` for Render health checks.
