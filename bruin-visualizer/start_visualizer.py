"""
Simple HTTP server to run the Bruin Pipeline Visualizer
"""

import http.server
import socketserver
import webbrowser
import os
from pathlib import Path

PORT = 8000

class MyHTTPRequestHandler(http.server.SimpleHTTPRequestHandler):
    def end_headers(self):
        # Add CORS headers to allow local file access
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Cache-Control', 'no-store, no-cache, must-revalidate')
        super().end_headers()

def main():
    # Change to the directory where the script is located
    os.chdir(Path(__file__).parent)
    
    Handler = MyHTTPRequestHandler
    
    with socketserver.TCPServer(("", PORT), Handler) as httpd:
        # Avoid emojis to prevent Windows console encoding errors
        print("Bruin Pipeline Visualizer with Impact Analysis")
        print(f"Server running at: http://localhost:{PORT}")
        print("Opening browser...")
        print(f"\nView your pipeline at: http://localhost:{PORT}/bruin-visualizer-history.html")
        print("\nPress Ctrl+C to stop the server\n")
        
        # Open browser
        webbrowser.open(f'http://localhost:{PORT}/bruin-visualizer-history.html')
        
        # Start server
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\n\n👋 Server stopped. Goodbye!")

if __name__ == "__main__":
    main()
