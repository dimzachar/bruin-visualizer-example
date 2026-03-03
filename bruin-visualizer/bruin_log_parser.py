"""
Bruin Log Parser
Parses bruin run output and stores in history database
"""

import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path
import uuid
from bruin_run_history import BruinRunHistory

class BruinLogParser:
    def __init__(self, db_path='bruin-visualizer/bruin_history.db', duckdb_config_path=None):
        self.db = BruinRunHistory(db_path)
        self.run_id = None
        self.pipeline_name = None
        self.started_at = None
        self.current_asset = None
        self.asset_start_times = {}
        self.duckdb_config_path = duckdb_config_path
    
    def parse_line(self, line: str):
        """Parse a single line of bruin run output"""
        
        # Strip ANSI color codes first (Bruin uses colored output)
        line_clean = re.sub(r'\x1b\[[0-9;]*m|\[[0-9;]*m', '', line)

        
        # Bruin format: [14:04:19] Running:  asset_name
        # Extract timestamp - Bruin uses [HH:MM:SS] format
        timestamp_match = re.search(r'\[(\d{2}:\d{2}:\d{2})\]', line_clean)
        if not timestamp_match:
            return
        
        time_str = timestamp_match.group(1)
        # Create full timestamp with today's date
        today = datetime.now().date()
        timestamp = datetime.strptime(f"{today} {time_str}", '%Y-%m-%d %H:%M:%S')
        
        # Pipeline start - detect from first "Running:" line
        if 'Running:' in line_clean and not self.run_id:
            self.pipeline_name = 'pipeline'
            self.run_id = str(uuid.uuid4())
            self.started_at = timestamp
            self.db.add_run(self.run_id, self.pipeline_name, timestamp)
            print(f"Started tracking run: {self.run_id}")
        
        # Asset start - Bruin format: "Running:  asset_name"
        if 'Running:' in line_clean and self.run_id:
            asset_match = re.search(r'Running:\s+(.+?)(?:\s|$)', line_clean)
            if asset_match:
                asset_name = asset_match.group(1).strip()
                self.current_asset = asset_name
                self.asset_start_times[asset_name] = timestamp
                self.db.add_asset_run(self.run_id, asset_name, timestamp)
                print(f"  > {asset_name}")
        
        # Asset success - Bruin format: "Finished: asset_name (duration)"
        elif 'Finished:' in line_clean and self.run_id:
            # Extract asset name and duration from "Finished: asset_name (6.492s)"
            finished_match = re.search(r'Finished:\s+(.+?)\s+\((\d+\.?\d*)s\)', line_clean)
            if finished_match:
                asset_name = finished_match.group(1).strip()
                duration = float(finished_match.group(2))
                
                # Extract rows if present
                rows_match = re.search(r'(\d+)\s*rows?', line_clean)
                rows = int(rows_match.group(1)) if rows_match else None
                
                self.db.update_asset_run(
                    self.run_id, 
                    asset_name, 
                    timestamp,
                    'success',
                    duration,
                    rows
                )
                print(f"  OK {asset_name}: {duration:.1f}s" + (f" ({rows:,} rows)" if rows else ""))
        
        # Asset failure
        elif '✗' in line_clean or 'failed' in line_clean.lower() or 'error' in line_clean.lower():
            if self.run_id and self.current_asset:
                # Extract error message
                error_match = re.search(r'(?:failed|error):?\s*(.+)', line_clean, re.IGNORECASE)
                error_msg = error_match.group(1).strip() if error_match else line_clean.strip()
                
                # Calculate duration
                duration = 0
                if self.current_asset in self.asset_start_times:
                    duration = (timestamp - self.asset_start_times[self.current_asset]).total_seconds()
                
                self.db.update_asset_run(
                    self.run_id,
                    self.current_asset,
                    timestamp,
                    'failed',
                    duration,
                    error_message=error_msg
                )
                print(f"  FAIL {self.current_asset}: Failed - {error_msg}")
        
        # Pipeline completion - Bruin format: "bruin run completed successfully in 1m7s"
        elif 'bruin run completed' in line_clean.lower():
            if self.run_id and self.started_at:
                # Extract duration from "1m7s" format
                duration_match = re.search(r'(\d+)m(\d+)s', line_clean)
                if duration_match:
                    minutes = int(duration_match.group(1))
                    seconds = int(duration_match.group(2))
                    duration = minutes * 60 + seconds
                else:
                    duration = (timestamp - self.started_at).total_seconds()
                
                status = 'success' if 'successfully' in line_clean.lower() else 'failed'
                self.db.update_run(self.run_id, timestamp, status, duration)
                print(f"Pipeline completed in {duration:.1f}s")
    
    def _find_duckdb(self, db_path: str) -> str:
        """Find DuckDB database file after the run completes"""
        # Bruin creates the DB relative to where it's executed
        # Try multiple possible locations
        possible_paths = [
            Path(db_path).resolve(),  # Current directory
            Path.cwd() / db_path,  # Explicit current directory
            Path.cwd().parent / 'duckdb.db',  # Parent directory
            Path.cwd().parent.parent / 'duckdb.db',  # Two levels up
            Path.cwd().parent.parent.parent / 'duckdb.db',  # Three levels up
        ]
        
        for path in possible_paths:
            if path.exists():
                print(f"Found DuckDB at: {path}")
                return str(path)
        
        print(f"Warning: Could not find DuckDB database. Row counts will not be populated.")
        return None
    
    def run_and_track(self, pipeline_path: str, flags: list = None):
        """Run bruin and track execution in real-time"""
        cmd = ['bruin', 'run', pipeline_path]
        if flags:
            cmd.extend(flags)
        
        print(f"Running: {' '.join(cmd)}")
        print("=" * 60)
        
        try:
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1
            )
            
            # Parse output line by line
            for line in process.stdout:
                line = line.rstrip()
                if line:
                    print(line)  # Show original output
                    self.parse_line(line)
            
            process.wait()
            
            # If we didn't catch pipeline completion, mark it now
            if self.run_id and self.started_at:
                completed_at = datetime.now()
                duration = (completed_at - self.started_at).total_seconds()
                status = 'success' if process.returncode == 0 else 'failed'
                
                # Mark any remaining "running" assets as complete
                # This handles assets that didn't have explicit "Finished:" messages
                self.db.finalize_incomplete_assets(self.run_id, completed_at, status)
                
                # Try to get row counts from DuckDB (search for it now after the run)
                if self.duckdb_config_path:
                    duckdb_path = self._find_duckdb(self.duckdb_config_path)
                    if duckdb_path:
                        self.db.update_row_counts_from_db(self.run_id, duckdb_path)
                
                self.db.update_run(self.run_id, completed_at, status, duration)
            
            return process.returncode
            
        except KeyboardInterrupt:
            print("\nRun cancelled by user")
            if self.run_id:
                self.db.update_run(self.run_id, datetime.now(), 'cancelled', 0)
            return 1
        except Exception as e:
            print(f"\nError: {e}")
            if self.run_id:
                self.db.update_run(self.run_id, datetime.now(), 'failed', 0, str(e))
            return 1


def main():
    if len(sys.argv) < 2:
        print("Usage: python bruin_log_parser.py <pipeline_path> [flags...]")
        print("\nExample:")
        print("  python bruin-visualizer\\bruin_log_parser.py pipeline --start-date 2019-01-01 --end-date 2019-01-31")
        print("  python bruin-visualizer\\bruin_log_parser.py pipeline --start-date 2022-01-01 --end-date 2022-01-31 --full-refresh --workers 4")
        sys.exit(1)
    
    pipeline_path = sys.argv[1]
    flags = sys.argv[2:] if len(sys.argv) > 2 else None
    
    # Get DuckDB config path (but don't search for it yet - it might not exist on first run)
    duckdb_config_path = None
    bruin_config = Path('.bruin.yml')
    if bruin_config.exists():
        try:
            import yaml
            with open(bruin_config) as f:
                config = yaml.safe_load(f)
                default_env = config.get('default_environment', 'default')
                connections = config.get('environments', {}).get(default_env, {}).get('connections', {})
                duckdb_conns = connections.get('duckdb', [])
                if duckdb_conns:
                    duckdb_config_path = duckdb_conns[0].get('path', 'duckdb.db')
        except Exception as e:
            print(f"Could not read DuckDB config: {e}")
    
    parser = BruinLogParser(duckdb_config_path=duckdb_config_path)
    exit_code = parser.run_and_track(pipeline_path, flags)
    
    print("\n" + "=" * 60)
    print("Run history saved to bruin-visualizer/bruin_history.db")
    print("View history: cd bruin-visualizer && python start_visualizer.py")
    
    sys.exit(exit_code)


if __name__ == '__main__':
    main()
