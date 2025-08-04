"""
Simple HTTP server to serve the portfolio dashboard locally.
This solves CORS issues when loading CSV and JSON files.
"""

import http.server
import os
import socketserver
import webbrowser
from pathlib import Path


def serve_dashboard(port=8000):
    """Serve the dashboard on localhost"""

    # Change to the script directory
    os.chdir(Path(__file__).parent)

    # Create handler
    handler = http.server.SimpleHTTPRequestHandler

    # Add CORS headers
    class CORSRequestHandler(handler):
        def end_headers(self):
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
            self.send_header("Access-Control-Allow-Headers", "*")
            super().end_headers()

    # Start server
    with socketserver.TCPServer(("", port), CORSRequestHandler) as httpd:
        print(f"🚀 Serving dashboard at http://localhost:{port}/portfolio_dashboard.html")
        print(f"📂 Files are being served from: {Path.cwd()}")
        print("Press Ctrl+C to stop the server")

        # Auto-open browser
        webbrowser.open(f"http://localhost:{port}/portfolio_dashboard.html")

        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\n👋 Server stopped")


if __name__ == "__main__":
    serve_dashboard()
