# Launch Checklist

## 1. Push the repo to GitHub

Push this folder as its own GitHub repository:

- `C:\Users\Admin\Documents\GitHub\njtransportationbids`

## 2. Deploy on Render

Create a new Render service from the GitHub repo.

- Service type: `Web Service`
- Runtime: `Docker`
- Dockerfile path: `./Dockerfile`
- Health check path: `/health`

Create a PostgreSQL database in Render and set:

- `DATABASE_URL`
- `ADMIN_USERNAME`
- `ADMIN_PASSWORD`

You can keep these app variables as-is:

- `APP_ENV=production`
- `PORT=10000`
- `HOST=0.0.0.0`
- `CRAWL_ENABLED=false`
- `LOG_LEVEL=info`

## 3. Verify the live service

After deploy completes, open:

- `https://<your-render-service>.onrender.com/health`
- `https://<your-render-service>.onrender.com/`

Expected `/health` response:

```json
{"ok": true}
```

## 4. Add your domain in Render

In the Render web service, add your custom domain first.
Render will show the exact DNS target value to use.

## 5. Point Cloudflare DNS to Render

In Cloudflare DNS:

- Remove any `AAAA` record for the same host if one exists.
- Add the DNS record Render asks for, usually a `CNAME`.
- Start with Cloudflare proxy disabled (`DNS only`) until SSL is working.

## 6. Set Cloudflare SSL mode

In Cloudflare SSL/TLS settings, use `Full`.

## 7. Final public checks

Confirm:

- `https://yourdomain.com/` loads
- `https://yourdomain.com/health` returns `{"ok": true}`
- Admin login works with your Render credentials
