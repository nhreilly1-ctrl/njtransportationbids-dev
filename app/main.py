from fastapi import FastAPI
from fastapi.responses import HTMLResponse

app = FastAPI(title="NJ Bid Registry")

@app.get("/", response_class=HTMLResponse)
def home():
    return """
    <html>
      <head>
        <title>NJ Transportation Bids</title>
        <style>
          body { font-family: Arial, sans-serif; margin: 40px; line-height: 1.5; }
          h1 { margin-bottom: 8px; }
          a { color: #0b57d0; text-decoration: none; }
          a:hover { text-decoration: underline; }
          .card { max-width: 700px; padding: 24px; border: 1px solid #ddd; border-radius: 12px; }
        </style>
      </head>
      <body>
        <div class="card">
          <h1>NJ Transportation Bids</h1>
          <p>The site is live. The full bid registry is being connected now.</p>
          <p><a href="/health">Health check</a></p>
          <p><a href="/ready">Readiness check</a></p>
        </div>
      </body>
    </html>
    """

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/ready")
def ready():
    return {"ok": True}
