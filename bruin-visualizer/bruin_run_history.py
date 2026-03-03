"""
Bruin Run History Tracker
Captures and stores pipeline run history in SQLite
"""

import sqlite3
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

class BruinRunHistory:
    def __init__(self, db_path='bruin_history.db'):
        self.db_path = db_path
        self.init_db()
    
    def init_db(self):
        """Initialize SQLite database with schema"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Runs table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS runs (
                run_id TEXT PRIMARY KEY,
                pipeline_name TEXT NOT NULL,
                started_at TIMESTAMP NOT NULL,
                completed_at TIMESTAMP,
                status TEXT CHECK(status IN ('running', 'success', 'failed', 'cancelled')),
                total_duration_seconds REAL,
                bruin_version TEXT,
                environment TEXT,
                flags TEXT,
                error_message TEXT
            )
        ''')
        
        # Asset runs table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS asset_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                asset_name TEXT NOT NULL,
                asset_type TEXT,
                started_at TIMESTAMP NOT NULL,
                completed_at TIMESTAMP,
                duration_seconds REAL,
                status TEXT CHECK(status IN ('queued', 'running', 'success', 'failed', 'skipped')),
                error_message TEXT,
                rows_affected INTEGER,
                FOREIGN KEY (run_id) REFERENCES runs(run_id)
            )
        ''')
        
        # Indexes for fast queries
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_asset_runs_asset_name ON asset_runs(asset_name)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_asset_runs_started_at ON asset_runs(started_at)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_runs_pipeline_started ON runs(pipeline_name, started_at)')
        
        conn.commit()
        conn.close()
    
    def add_run(self, run_id: str, pipeline_name: str, started_at: datetime, 
                status: str = 'running', flags: Dict = None):
        """Add a new pipeline run"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO runs (run_id, pipeline_name, started_at, status, flags)
            VALUES (?, ?, ?, ?, ?)
        ''', (run_id, pipeline_name, started_at.isoformat(), status, json.dumps(flags or {})))
        
        conn.commit()
        conn.close()
    
    def update_run(self, run_id: str, completed_at: datetime, status: str, 
                   duration_seconds: float, error_message: str = None):
        """Update run with completion info"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            UPDATE runs 
            SET completed_at = ?, status = ?, total_duration_seconds = ?, error_message = ?
            WHERE run_id = ?
        ''', (completed_at.isoformat(), status, duration_seconds, error_message, run_id))
        
        conn.commit()
        conn.close()
    
    def add_asset_run(self, run_id: str, asset_name: str, started_at: datetime,
                      asset_type: str = None, status: str = 'running'):
        """Add an asset run"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO asset_runs (run_id, asset_name, asset_type, started_at, status)
            VALUES (?, ?, ?, ?, ?)
        ''', (run_id, asset_name, asset_type, started_at.isoformat(), status))
        
        conn.commit()
        conn.close()
    
    def update_asset_run(self, run_id: str, asset_name: str, completed_at: datetime,
                        status: str, duration_seconds: float, rows_affected: int = None,
                        error_message: str = None):
        """Update asset run with completion info"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            UPDATE asset_runs 
            SET completed_at = ?, status = ?, duration_seconds = ?, 
                rows_affected = ?, error_message = ?
            WHERE run_id = ? AND asset_name = ?
        ''', (completed_at.isoformat(), status, duration_seconds, rows_affected, 
              error_message, run_id, asset_name))
        
        conn.commit()
        conn.close()
    
    def finalize_incomplete_assets(self, run_id: str, completed_at: datetime, status: str):
        """Mark any assets still in 'running' state as complete"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get all assets still in running state for this run
        cursor.execute('''
            SELECT asset_name, started_at 
            FROM asset_runs 
            WHERE run_id = ? AND status = 'running' AND completed_at IS NULL
        ''', (run_id,))
        
        incomplete_assets = cursor.fetchall()
        
        # Update each incomplete asset
        for asset_name, started_at_str in incomplete_assets:
            started_at = datetime.fromisoformat(started_at_str)
            duration = (completed_at - started_at).total_seconds()
            
            cursor.execute('''
                UPDATE asset_runs 
                SET completed_at = ?, status = ?, duration_seconds = ?
                WHERE run_id = ? AND asset_name = ? AND status = 'running'
            ''', (completed_at.isoformat(), status, duration, run_id, asset_name))
        
        conn.commit()
        conn.close()
        
        if incomplete_assets:
            print(f"Finalized {len(incomplete_assets)} incomplete assets")
    
    def update_row_counts_from_db(self, run_id: str, db_path: str):
        """Query DuckDB to get row counts for tables created in this run"""
        try:
            import duckdb
            
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Get all asset names from this run (excluding quality checks)
            cursor.execute('''
                SELECT DISTINCT asset_name 
                FROM asset_runs 
                WHERE run_id = ? AND asset_name NOT LIKE '%:%'
            ''', (run_id,))
            
            assets = [row[0] for row in cursor.fetchall()]
            
            # Connect to DuckDB
            duck_conn = duckdb.connect(db_path, read_only=True)
            
            updated_count = 0
            for asset_name in assets:
                # Asset names like "staging.stg_yellow_tripdata" map to schema.table
                parts = asset_name.split('.', 1)
                if len(parts) == 2:
                    schema, table = parts
                    
                    try:
                        result = duck_conn.execute(f"SELECT COUNT(*) FROM {schema}.{table}").fetchone()
                        if result:
                            row_count = result[0]
                            cursor.execute('''
                                UPDATE asset_runs 
                                SET rows_affected = ?
                                WHERE run_id = ? AND asset_name = ?
                            ''', (row_count, run_id, asset_name))
                            updated_count += 1
                    except:
                        # Table might not exist or have different name
                        pass
            
            duck_conn.close()
            conn.commit()
            conn.close()
            
            if updated_count > 0:
                print(f"Updated row counts for {updated_count} assets")
                
        except ImportError:
            pass  # DuckDB not installed, skip row count updates
        except Exception as e:
            pass  # Silently fail if we can't get row counts
    
    def get_recent_runs(self, pipeline_name: str, limit: int = 30) -> List[Dict]:
        """Get recent pipeline runs"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT * FROM runs
            WHERE pipeline_name = ?
            ORDER BY started_at DESC
            LIMIT ?
        ''', (pipeline_name, limit))
        
        runs = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return runs
    
    def get_asset_history(self, asset_name: str, days: int = 30) -> List[Dict]:
        """Get run history for a specific asset"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT * FROM asset_runs
            WHERE asset_name = ?
              AND started_at >= datetime('now', '-' || ? || ' days')
            ORDER BY started_at DESC
        ''', (asset_name, days))
        
        history = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return history
    
    def get_run_stats(self, pipeline_name: str, days: int = 30) -> Dict:
        """Get statistics for pipeline runs"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT 
                COUNT(*) as total_runs,
                AVG(total_duration_seconds) as avg_duration,
                MIN(total_duration_seconds) as min_duration,
                MAX(total_duration_seconds) as max_duration,
                SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as success_count,
                SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed_count
            FROM runs
            WHERE pipeline_name = ?
              AND started_at >= datetime('now', '-' || ? || ' days')
              AND status IN ('success', 'failed')
        ''', (pipeline_name, days))
        
        row = cursor.fetchone()
        conn.close()
        
        if row and row[0] > 0:
            return {
                'total_runs': row[0],
                'avg_duration': row[1],
                'min_duration': row[2],
                'max_duration': row[3],
                'success_count': row[4],
                'failed_count': row[5],
                'success_rate': (row[4] / row[0]) * 100 if row[0] > 0 else 0
            }
        return None
    
    def get_asset_stats(self, asset_name: str, days: int = 30) -> Dict:
        """Get statistics for a specific asset"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT 
                COUNT(*) as total_runs,
                AVG(duration_seconds) as avg_duration,
                MIN(duration_seconds) as min_duration,
                MAX(duration_seconds) as max_duration,
                SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as success_count,
                SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed_count,
                AVG(rows_affected) as avg_rows
            FROM asset_runs
            WHERE asset_name = ?
              AND started_at >= datetime('now', '-' || ? || ' days')
              AND status IN ('success', 'failed')
        ''', (asset_name, days))
        
        row = cursor.fetchone()
        conn.close()
        
        if row and row[0] > 0:
            return {
                'total_runs': row[0],
                'avg_duration': row[1],
                'min_duration': row[2],
                'max_duration': row[3],
                'success_count': row[4],
                'failed_count': row[5],
                'success_rate': (row[4] / row[0]) * 100 if row[0] > 0 else 0,
                'avg_rows': row[6]
            }
        return None
    
    def get_failure_patterns(self, asset_name: str, days: int = 30) -> List[Dict]:
        """Get common failure patterns for an asset"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT 
                error_message,
                COUNT(*) as occurrence_count,
                MAX(started_at) as last_occurrence
            FROM asset_runs
            WHERE asset_name = ?
              AND status = 'failed'
              AND started_at >= datetime('now', '-' || ? || ' days')
              AND error_message IS NOT NULL
            GROUP BY error_message
            ORDER BY occurrence_count DESC
            LIMIT 5
        ''', (asset_name, days))
        
        patterns = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return patterns
    
    def export_to_json(self, pipeline_name: str) -> Dict:
        """Export all run history to JSON for web UI"""
        runs = self.get_recent_runs(pipeline_name, limit=100)
        stats = self.get_run_stats(pipeline_name)
        
        # Get all unique assets from recent runs
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            SELECT DISTINCT asset_name 
            FROM asset_runs 
            WHERE run_id IN (
                SELECT run_id FROM runs 
                WHERE pipeline_name = ? 
                ORDER BY started_at DESC 
                LIMIT 100
            )
        ''', (pipeline_name,))
        
        assets = [row[0] for row in cursor.fetchall()]
        conn.close()
        
        # Get history for each asset
        asset_histories = {}
        for asset in assets:
            asset_histories[asset] = {
                'history': self.get_asset_history(asset, days=30),
                'stats': self.get_asset_stats(asset, days=30),
                'failures': self.get_failure_patterns(asset, days=30)
            }
        
        return {
            'runs': runs,
            'stats': stats,
            'assets': asset_histories
        }


if __name__ == '__main__':
    # Test the database
    db = BruinRunHistory()
    print("✅ Database initialized successfully")
    print(f"📁 Database location: {Path(db.db_path).absolute()}")
