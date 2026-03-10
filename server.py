#!/usr/bin/env python3
"""
LG Buying Guides Dashboard Server
Simple HTTP server with data refresh endpoint.
"""

import http.server
import json
import os
import subprocess
import sys
from datetime import datetime
from urllib.parse import urlparse

PORT = 8080
DATA_DIR = os.path.dirname(os.path.abspath(__file__))


class DashboardHandler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=DATA_DIR, **kwargs)

    def do_GET(self):
        parsed = urlparse(self.path)

        if parsed.path == '/':
            self.path = '/index.html'
            return super().do_GET()

        if parsed.path == '/api/refresh':
            self.handle_refresh()
            return

        if parsed.path == '/api/data':
            self.handle_data()
            return

        return super().do_GET()

    def handle_refresh(self):
        """Run parse_data.py and return updated data."""
        try:
            result = subprocess.run(
                [sys.executable, os.path.join(DATA_DIR, 'parse_data.py')],
                capture_output=True, text=True, timeout=60
            )

            if result.returncode != 0:
                self.send_json(500, {
                    'status': 'error',
                    'message': result.stderr or 'Parse script failed'
                })
                return

            # Read the generated data.json and add timestamp
            data_path = os.path.join(DATA_DIR, 'data.json')
            with open(data_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            data['meta']['last_updated'] = datetime.now().isoformat()

            with open(data_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

            self.send_json(200, {
                'status': 'success',
                'message': 'Data refreshed successfully',
                'log': result.stdout,
                'timestamp': data['meta']['last_updated']
            })

        except subprocess.TimeoutExpired:
            self.send_json(500, {'status': 'error', 'message': 'Parse script timed out'})
        except Exception as e:
            self.send_json(500, {'status': 'error', 'message': str(e)})

    def handle_data(self):
        """Serve data.json."""
        data_path = os.path.join(DATA_DIR, 'data.json')
        if not os.path.exists(data_path):
            self.send_json(404, {'status': 'error', 'message': 'No data file found. Click Refresh.'})
            return

        with open(data_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        self.send_json(200, data)

    def send_json(self, code, data):
        self.send_response(code)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps(data, ensure_ascii=False).encode('utf-8'))

    def log_message(self, format, *args):
        if '/api/' in str(args[0]) if args else False:
            super().log_message(format, *args)


def main():
    print(f"\n{'='*60}")
    print(f"  LG Buying Guides Dashboard")
    print(f"  http://localhost:{PORT}")
    print(f"{'='*60}\n")

    server = http.server.HTTPServer(('', PORT), DashboardHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nServer stopped.")
        server.server_close()


if __name__ == '__main__':
    main()
