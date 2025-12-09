# api/Run_Automation.py
from http.server import BaseHTTPRequestHandler
import json
from lib import Norway_Automation as na

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            result = na.run_scrape(None)

            body = json.dumps(result, ensure_ascii=False).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        except Exception as e:
            err = {"error": repr(e)}
            body = json.dumps(err).encode("utf-8")
            self.send_response(500)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
