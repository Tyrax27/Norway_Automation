import os, json, traceback
from http.server import BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

from Norway_Automation import handler as scrape_handler


class handler(BaseHTTPRequestHandler):
    """
    Vercel Python Serverless Function entrypoint.
    Route: /api/run
    Supports GET and POST.
    """

    def do_GET(self):
        self._handle()

    def do_POST(self):
        self._handle()

    def _handle(self):
        # ---- Optional simple auth (recommended) ----
        # Set RUN_TOKEN in Vercel env.
        expected = os.environ.get("RUN_TOKEN")
        qs = parse_qs(urlparse(self.path).query)
        provided = self.headers.get("x-run-token") or qs.get("token", [None])[0]

        if expected and provided != expected:
            self._send_json(401, {"error": "unauthorized"})
            return

        try:
            result = scrape_handler(None)
            self._send_json(200, result)
        except Exception as e:
            self._send_json(500, {
                "error": str(e),
                "trace": traceback.format_exc()
            })

    def _send_json(self, status, payload):
        body = json.dumps(payload, ensure_ascii=False)
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.end_headers()
        self.wfile.write(body.encode("utf-8"))
