# Bruin Pipeline Visualizer

> Free, web-based visualizer for Bruin CLI pipelines with impact analysis and run history tracking

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## What This Does

A standalone web-based visualizer for Bruin data pipelines. Visualize your pipeline DAG, analyze asset criticality, and track run history - all locally without requiring Bruin Cloud.

## Features

### Core Visualization
- **Interactive DAG Visualization** - D3.js force-directed graph with zoom, pan, and drag
- **Impact Analysis** - Automated criticality scoring (0-10 scale) with downstream dependency tracking
- **Run History Tracking** - Track asset performance over time with SQLite storage
- **Performance Trends** - Sparkline charts showing duration trends, failure patterns, and alerts
- **Asset Metadata** - View descriptions, owners, tags, column definitions, and materialization info
- **Search** - Multi-field search with keyboard shortcuts (Ctrl+K)

## Quick Start

### Prerequisites
- Python 3.7+
- A `pipeline_graph.json` file from your Bruin pipeline
- PyYAML (optional, for automatic row count tracking from DuckDB)

Install dependencies:
```bash
pip install pyyaml duckdb
```

### End-to-end commands for this NYC Taxi project

Run commands from the project root directory (where `.bruin.yml` is located).

1. **First run - Initial data load**

```bash
# From the project root (e.g., nyc-taxi/)
python bruin-visualizer\bruin_log_parser.py pipeline --start-date 2019-01-01 --end-date 2019-01-31 --workers 1
```

This runs the pipeline for January 2019 data and records the run history in `bruin-visualizer\bruin_history.db`.

2. **Generate the pipeline graph for the visualizer**

```bash
cd bruin-visualizer
python bruin_parser.py ..\pipeline
cd ..
```

This creates `bruin-visualizer\pipeline_graph.json`, which the UI needs to render the DAG and impact analysis.

3. **Start the visualizer server**

```bash
cd bruin-visualizer
python bruin_history_api.py
```

This starts a server on port 8001 that:
- Serves the visualizer web UI
- Provides API endpoints for run history data
- Automatically opens your browser

4. **Open the visualizer**

The browser should open automatically, or visit:

```text
http://localhost:8001/bruin-visualizer-history.html
```

You should see:
- The NYC Taxi DAG from `pipeline_graph.json`
- Real run history and trends pulled from `bruin_history.db` via the API

**Note:** If you only want to view the pipeline DAG without history data:
1. Run your pipeline normally with `bruin run pipeline --start-date 2019-01-01 --end-date 2019-01-31 --workers 1` (no need to use the log parser wrapper)
2. Generate the pipeline graph (step 2)
3. Use 
```bash
cd bruin-visualizer
python start_visualizer.py 
```
This starts a server on port 8000.

## Project Structure

```
bruin-visualizer/
├── bruin-visualizer-history.html  # Main visualizer UI
├── bruin_history_api.py          # API server (port 8001)
├── bruin_run_history.py          # SQLite database for run history
├── start_visualizer.py           # Web server (port 8000)
├── requirements.txt              # Python dependencies (PyYAML)
└── README.md                     # This file
```

## How It Works

1. **Pipeline Data**: Place your `pipeline_graph.json` in the same directory (or update the HTML to point to your file location)
2. **Run History**: The first time you click an asset, the API fetches its 30-day run history from the SQLite database
3. **Visualization**: D3.js renders the pipeline as an interactive force-directed graph
4. **Impact Analysis**: Criticality scores are calculated based on downstream dependencies

## API Endpoints

The API server (port 8001) provides these endpoints:

```bash
# Get recent pipeline runs
GET /api/runs?pipeline=nyc-taxi&limit=30

# Get asset history (30 days)
GET /api/asset-history?asset=marts.fct_trips&days=30

# Get pipeline statistics
GET /api/stats?pipeline=nyc-taxi&days=30

# Export all data
GET /api/export?pipeline=nyc-taxi
```

## Configuration

### Change Server Ports

**Web server (start_visualizer.py):**
```python
PORT = 8000  # Change to your preferred port
```

**API server (bruin_history_api.py):**
```python
def start_server(port=8001):  # Change port parameter
```

### Update Pipeline Graph Location

In `bruin-visualizer-history.html`, line ~942:
```javascript
const response = await fetch('pipeline_graph.json');  // Update path here
```

## Data Format

### pipeline_graph.json

The visualizer expects a JSON file with this structure:

```json
{
  "nodes": [
    {
      "id": "stg_yellow_tripdata",
      "name": "stg_yellow_tripdata",
      "layer": "staging",
      "description": "Staging table for yellow taxi trips",
      "owner": "data-team",
      "tags": ["taxi", "staging"]
    }
  ],
  "links": [
    {
      "source": "stg_yellow_tripdata",
      "target": "fct_trips"
    }
  ]
}
```

### Run History Database

Run history is stored in `bruin_history.db` (SQLite). The database is created automatically on first run.

## Troubleshooting

### Port Already in Use
```bash
# Find the process using port 8001 (Windows)
netstat -ano | findstr :8001

# Kill the process
taskkill /PID <process_id> /F
```

### No Pipeline Graph
Make sure `pipeline_graph.json` exists in the same directory as the HTML file, or update the fetch path in the JavaScript.

### No Run History Data
The database starts empty. You'll need to populate it with your pipeline run data using the `BruinRunHistory` class from `bruin_run_history.py`.

## Technology Stack

- **Frontend**: D3.js (force-directed graph), vanilla JavaScript
- **Backend**: Python `http.server` (standard library)
- **Database**: SQLite (via Python `sqlite3`)
- **Dependencies**: PyYAML (optional, for advanced parsing)

## Key Files

| File | Purpose | Port |
|------|---------|------|
| `start_visualizer.py` | Static file server for HTML/JS | 8000 |
| `bruin_history_api.py` | REST API for run history data | 8001 |
| `bruin_run_history.py` | SQLite database manager | - |
| `bruin_log_parser.py` | Parse Bruin run logs and populate history | - |
| `bruin_parser.py` | Generate pipeline_graph.json from pipeline | - |
| `bruin-visualizer-history.html` | Main UI (D3.js visualization) | - |
| `bruin_history.db` | SQLite database (auto-created) | - |
| `pipeline_graph.json` | Pipeline data (generated) | - |

## Use Cases

### 1. Pre-Deployment Impact Analysis
**Scenario:** "I need to change `stg_yellow_tripdata`. What breaks?"

**Solution:**
1. Click the asset in the visualizer
2. See: HIGH impact (9.6/10), affects 8 assets, 2 production reports
3. Export impact report
4. Share with team
5. Schedule deployment during maintenance window

**Result:** No surprises, no broken dashboards

### 2. Finding Assets Quickly
**Scenario:** "Where's the monthly revenue report?"

**Solution:**
1. Press Ctrl+K
2. Type "monthly revenue"
3. Click result
4. See full details + impact

**Result:** Found in 3 seconds

### 3. Performance Monitoring
**Scenario:** "Is this asset getting slower over time?"

**Solution:**
1. Click the asset
2. View sparkline chart showing duration trends
3. See alerts if 40% slower than baseline
4. Review recent run history

**Result:** Catch performance degradation early

### 4. Understanding Pipeline Structure
**Scenario:** "New team member needs to understand the pipeline"

**Solution:**
1. Open visualizer
2. See color-coded layers
3. Click nodes to see descriptions
4. Follow connections
5. Search by layer/tag

**Result:** Understand pipeline in 10 minutes

## Development

The visualizer is designed to be simple and standalone:

1. No build step required
2. No npm/node dependencies
3. Uses only Python standard library (plus optional PyYAML)
4. Single HTML file with embedded CSS/JS


## License

MIT License - feel free to use, modify, and distribute.

## Acknowledgments

- Built for [Bruin](https://github.com/bruin-data/bruin) data pipelines
- Visualization powered by [D3.js](https://d3js.org/)
- Inspired by dbt Docs and Bruin Cloud

## Contributing

Contributions welcome! This project focuses on filling real gaps in the Bruin ecosystem.

### Priority Areas

**High Impact Features:**
1. **Critical path highlighting** - Shows which bottleneck actually matters
2. **Row count funnel** - Unique data flow visualization
3. **Performance trend analysis** - Catch degradation early
4. **Layout modes** - Better for presentations and documentation

**Code Quality:**
- Add tests for impact analysis algorithm
- Improve error handling in log parser
- Add TypeScript definitions for better IDE support
- Optimize D3.js rendering for large graphs (100+ nodes)

**Documentation:**
- Add video walkthrough
- Create tutorial for first-time users
- Document API endpoints
- Add troubleshooting guide

### What NOT to Contribute

To maintain focus and avoid feature bloat:

❌ **Real-time monitoring** - Competes with Bruin Cloud
❌ **Column-level lineage** - Already in Bruin CLI
❌ **Data quality dashboard** - Already in Bruin
❌ **Cost tracking** - Requires cloud integration

### How to Contribute

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/critical-path`)
3. Make your changes
4. Test with a real Bruin pipeline
5. Submit a pull request with:
   - Clear description of the feature
   - Screenshots/GIFs if UI changes
   - Test results
   - Documentation updates

### Development Setup

```bash
# Clone the repo
git clone https://github.com/yourusername/bruin-visualizer.git
cd bruin-visualizer

# Install dependencies
pip install pyyaml duckdb

# Generate test data
python bruin_parser.py ../pipeline

# Start the server
python bruin_history_api.py

# Open in browser
# http://localhost:8001/bruin-visualizer-history.html
```

### Testing

```bash
# Test with your own pipeline
python bruin_parser.py /path/to/your/pipeline

# Test log parser
python bruin_log_parser.py pipeline --start-date 2024-01-01 --end-date 2024-01-31

# Verify database
sqlite3 bruin_history.db "SELECT COUNT(*) FROM runs;"
```

### Code Style

- Python: Follow PEP 8
- JavaScript: Use ES6+ features
- Comments: Explain why, not what
- Functions: Keep them small and focused
- Naming: Be descriptive (no abbreviations)
