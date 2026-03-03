"""
API endpoint to serve run history data to web UI
"""

from http.server import HTTPServer, SimpleHTTPRequestHandler
import json
from urllib.parse import urlparse, parse_qs
from bruin_run_history import BruinRunHistory

class HistoryAPIHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        self.db = BruinRunHistory()
        super().__init__(*args, **kwargs)
    
    def do_GET(self):
        parsed_path = urlparse(self.path)
        path = parsed_path.path
        query = parse_qs(parsed_path.query)
        
        # API endpoints
        if path == '/api/runs':
            self.serve_runs(query)
        elif path == '/api/asset-history':
            self.serve_asset_history(query)
        elif path == '/api/stats':
            self.serve_stats(query)
        elif path == '/api/export':
            self.serve_export(query)
        else:
            # Serve static files
            super().do_GET()
    
    def serve_runs(self, query):
        """Get recent pipeline runs"""
        pipeline = query.get('pipeline', ['nyc-taxi'])[0]
        limit = int(query.get('limit', [30])[0])
        
        runs = self.db.get_recent_runs(pipeline, limit)
        
        self.send_json_response(runs)
    
    def serve_asset_history(self, query):
        """Get history for a specific asset"""
        asset_name = query.get('asset', [None])[0]
        days = int(query.get('days', [30])[0])
        
        if not asset_name:
            self.send_error(400, "Missing 'asset' parameter")
            return
        
        history = self.db.get_asset_history(asset_name, days)
        stats = self.db.get_asset_stats(asset_name, days)
        failures = self.db.get_failure_patterns(asset_name, days)
        
        response = {
            'asset_name': asset_name,
            'history': history,
            'stats': stats,
            'failures': failures
        }
        
        self.send_json_response(response)
    
    def serve_stats(self, query):
        """Get pipeline statistics"""
        pipeline = query.get('pipeline', ['nyc-taxi'])[0]
        days = int(query.get('days', [30])[0])
        
        stats = self.db.get_run_stats(pipeline, days)
        
        self.send_json_response(stats)
    
    def serve_export(self, query):
        """Export all data for a pipeline"""
        pipeline = query.get('pipeline', ['nyc-taxi'])[0]
        
        data = self.db.export_to_json(pipeline)
        
        self.send_json_response(data)
    
    def send_json_response(self, data):
        """Send JSON response"""
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps(data, indent=2).encode())
    
    def log_message(self, format, *args):
        """Custom log format"""
        print(f"[API] {args[0]}")


def start_server(port=8001):
    """Start the API server"""
    server_address = ('', port)
    httpd = HTTPServer(server_address, HistoryAPIHandler)
    # Avoid emojis in logs to prevent Windows console encoding errors
    print(f"Run History API server running on http://localhost:{port}")
    print("\nAvailable endpoints:")
    print(f"  GET /api/runs?pipeline=nyc-taxi&limit=30")
    print(f"  GET /api/asset-history?asset=marts.fct_trips&days=30")
    print(f"  GET /api/stats?pipeline=nyc-taxi&days=30")
    print(f"  GET /api/export?pipeline=nyc-taxi")
    print(f"\nWeb UI: http://localhost:{port}/bruin-visualizer-history.html")
    print("\nPress Ctrl+C to stop\n")
    
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n\n👋 Server stopped")
        httpd.shutdown()


if __name__ == '__main__':
    start_server()
