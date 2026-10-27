"""Loopback-only UI and JSON API. Never executes SQL or invokes dbt."""
from __future__ import annotations

import argparse
import json
import socket
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

from converter import Converter

ROOT = Path(__file__).resolve().parent
DEFAULT_REPO = ROOT.parent / 'km-etl-newcore'


class LocalHTTPServer(ThreadingHTTPServer):
    allow_reuse_address = False

    def server_bind(self):
        if hasattr(socket, 'SO_EXCLUSIVEADDRUSE'):
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
        super().server_bind()


def make_handler(converter: Converter):
    lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        def respond(self, status, payload, content_type='application/json; charset=utf-8'):
            body = json.dumps(payload, ensure_ascii=False).encode() if isinstance(payload, dict) else payload
            self.send_response(status)
            self.send_header('Content-Type', content_type)
            self.send_header('Content-Length', str(len(body)))
            self.send_header('X-Content-Type-Options', 'nosniff')
            self.send_header('Cache-Control', 'no-store')
            self.send_header('Content-Security-Policy', "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; frame-ancestors 'none'")
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            path = urlparse(self.path).path
            if path == '/api/health':
                self.respond(200, {'status': 'ok', 'repo': str(converter.repo), 'metadata': converter.summary()})
                return
            files = {'/': ('index.html', 'text/html; charset=utf-8'), '/app.js': ('app.js', 'text/javascript; charset=utf-8'), '/style.css': ('style.css', 'text/css; charset=utf-8')}
            if path not in files:
                self.respond(404, {'error': 'Not found'})
                return
            name, mime = files[path]
            self.respond(200, (ROOT / 'static' / name).read_bytes(), mime)

        def do_POST(self):
            if self.path != '/api/convert':
                self.respond(404, {'error': 'Not found'})
                return
            # Reject cross-origin requests; the API only serves this local UI.
            origin = self.headers.get('Origin')
            if origin and origin != f'http://{self.headers.get("Host")}':
                self.respond(403, {'error': 'Cross-origin request rejected'})
                return
            try:
                size = int(self.headers.get('Content-Length', '0'))
                if size <= 0 or size > 2_000_000:
                    self.respond(413, {'error': 'Request must be between 1 byte and 2 MB.'})
                    return
                payload = json.loads(self.rfile.read(size))
                if not isinstance(payload, dict):
                    raise ValueError('Expected a JSON object.')
                with lock:
                    result = converter.convert(payload)
                self.respond(200, result)
            except (ValueError, TypeError) as exc:
                self.respond(400, {'error': str(exc)})
            except Exception as exc:
                self.log_error('Conversion failed: %s', type(exc).__name__)
                self.respond(500, {'error': 'Unexpected conversion error. Check the local terminal.'})

    return Handler


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--port', type=int, default=8001)
    parser.add_argument('--repo', type=Path, default=DEFAULT_REPO)
    args = parser.parse_args()
    converter = Converter(args.repo)
    try:
        server = LocalHTTPServer(('127.0.0.1', args.port), make_handler(converter))
    except OSError as exc:
        parser.exit(1, f'Cannot bind 127.0.0.1:{args.port}. The port may already be in use. {exc}\n')
    print(f'dbt -> Athena: http://127.0.0.1:{args.port}', flush=True)
    print(f'Read-only metadata: {converter.repo}', flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == '__main__':
    main()
