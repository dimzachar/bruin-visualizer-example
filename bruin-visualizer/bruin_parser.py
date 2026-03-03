"""
Bruin Pipeline Visualizer - Data Parser
Extracts and processes Bruin lineage data for visualization
"""

import json
import subprocess
import re
from pathlib import Path
from typing import Dict, List, Any


class BruinParser:
    """Parse Bruin CLI outputs and pipeline metadata"""
    
    def __init__(self, pipeline_path: str):
        self.pipeline_path = Path(pipeline_path)
        self.assets_path = self.pipeline_path / "assets"

    def get_asset_lineage(self, asset_path: str, full: bool = True) -> Dict[str, Any]:
        """Get lineage for a specific asset using Bruin CLI"""
        cmd = ["bruin", "lineage", asset_path, "--output", "json"]
        if full:
            cmd.append("--full")
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            return json.loads(result.stdout)
        except subprocess.CalledProcessError as e:
            print(f"Error getting lineage for {asset_path}: {e.stderr}")
            return {}
    
    def discover_all_assets(self) -> List[Path]:
        """Discover all asset files in the pipeline"""
        asset_files = []
        
        # SQL files
        asset_files.extend(self.assets_path.rglob("*.sql"))
        
        # Python files
        asset_files.extend(self.assets_path.rglob("*.py"))
        
        # YAML asset files
        asset_files.extend(self.assets_path.rglob("*.asset.yml"))
        asset_files.extend(self.assets_path.rglob("*.asset.yaml"))
        
        # Filter out __pycache__ and other non-asset files
        asset_files = [f for f in asset_files if "__pycache__" not in str(f)]
        
        return asset_files
    
    def build_full_pipeline_graph(self) -> Dict[str, Any]:
        """Build complete pipeline graph from all assets"""
        assets = self.discover_all_assets()
        
        nodes = {}
        edges = []
        
        print(f"Discovered {len(assets)} assets")
        
        for asset_file in assets:
            print(f"Processing: {asset_file.name}")
            lineage = self.get_asset_lineage(str(asset_file), full=False)
            
            if not lineage:
                continue
            
            # Add current node
            asset_name = lineage.get("name")
            asset_type = lineage.get("type")
            
            # Parse metadata based on file type
            metadata = {}
            if asset_file.suffix == ".sql":
                metadata = self.parse_sql_metadata(asset_file)
            elif asset_file.suffix in [".yml", ".yaml"]:
                metadata = self.parse_yaml_metadata(asset_file)
            elif asset_file.suffix == ".py":
                metadata = self.parse_python_metadata(asset_file)
            
            if asset_name not in nodes:
                nodes[asset_name] = {
                    "id": asset_name,
                    "name": asset_name,
                    "type": asset_type,
                    "layer": asset_name.split(".")[0] if "." in asset_name else "unknown",
                    "file": str(asset_file.relative_to(self.pipeline_path)),
                    "metadata": metadata
                }
            
            # Add upstream connections
            for upstream in lineage.get("upstreams", []):
                upstream_name = upstream.get("name")
                if upstream_name not in nodes:
                    nodes[upstream_name] = {
                        "id": upstream_name,
                        "name": upstream_name,
                        "type": upstream.get("type"),
                        "layer": upstream_name.split(".")[0] if "." in upstream_name else "unknown"
                    }
                
                edges.append({
                    "source": upstream_name,
                    "target": asset_name
                })
            
            # Add downstream connections
            for downstream in lineage.get("downstream", []):
                downstream_name = downstream.get("name")
                if downstream_name not in nodes:
                    nodes[downstream_name] = {
                        "id": downstream_name,
                        "name": downstream_name,
                        "type": downstream.get("type"),
                        "layer": downstream_name.split(".")[0] if "." in downstream_name else "unknown"
                    }
                
                edges.append({
                    "source": asset_name,
                    "target": downstream_name
                })
        
        return {
            "nodes": list(nodes.values()),
            "edges": edges,
            "stats": {
                "total_assets": len(nodes),
                "total_connections": len(edges),
                "layers": list(set(n["layer"] for n in nodes.values()))
            }
        }
    
    def get_pipeline_metadata(self) -> Dict[str, Any]:
        """Extract metadata from pipeline.yml"""
        pipeline_yml = self.pipeline_path / "pipeline.yml"
        
        if not pipeline_yml.exists():
            return {}
        
        try:
            import yaml
            with open(pipeline_yml, 'r') as f:
                return yaml.safe_load(f)
        except ImportError:
            print("PyYAML not installed. Install with: pip install pyyaml")
            return {}
        except Exception as e:
            print(f"Error reading pipeline.yml: {e}")
            return {}
    
    def parse_sql_metadata(self, file_path: Path) -> Dict[str, Any]:
        """Parse metadata from SQL file with /* @bruin ... @bruin */ block"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Extract @bruin block
            bruin_match = re.search(r'/\*\s*@bruin\s*(.*?)\s*@bruin\s*\*/', content, re.DOTALL)
            if not bruin_match:
                return {}
            
            bruin_content = bruin_match.group(1)
            
            # Parse YAML-like content
            metadata = {
                'description': '',
                'owner': '',
                'tags': [],
                'columns': [],
                'materialization': {},
                'depends': []
            }
            
            # Extract description (first comment after @bruin block)
            desc_match = re.search(r'@bruin\s*\*/\s*\n\s*--\s*(.+)', content)
            if desc_match:
                metadata['description'] = desc_match.group(1).strip()
            
            # Extract owner
            owner_match = re.search(r'owner:\s*(.+)', bruin_content)
            if owner_match:
                metadata['owner'] = owner_match.group(1).strip()
            
            # Extract tags
            tags_match = re.search(r'tags:\s*\n((?:\s*-\s*.+\n?)+)', bruin_content)
            if tags_match:
                tags_text = tags_match.group(1)
                metadata['tags'] = [line.strip('- \n') for line in tags_text.split('\n') if line.strip()]
            
            # Extract materialization
            mat_match = re.search(r'materialization:\s*\n\s*type:\s*(\w+)', bruin_content)
            if mat_match:
                metadata['materialization'] = {'type': mat_match.group(1)}
            
            # Extract dependencies
            deps_match = re.search(r'depends:\s*\n((?:\s*-\s*.+\n?)+)', bruin_content)
            if deps_match:
                deps_text = deps_match.group(1)
                metadata['depends'] = [line.strip('- \n') for line in deps_text.split('\n') if line.strip()]
            
            return metadata
        except Exception as e:
            print(f"Error parsing SQL metadata from {file_path}: {e}")
            return {}
    
    def parse_yaml_metadata(self, file_path: Path) -> Dict[str, Any]:
        """Parse metadata from YAML asset file"""
        try:
            import yaml
            with open(file_path, 'r', encoding='utf-8') as f:
                data = yaml.safe_load(f)
            
            return {
                'description': data.get('description', ''),
                'owner': data.get('owner', ''),
                'tags': data.get('tags', []),
                'columns': data.get('columns', []),
                'materialization': data.get('materialization', {}),
                'depends': [d if isinstance(d, str) else d.get('asset', '') for d in data.get('depends', [])],
                'parameters': data.get('parameters', {})
            }
        except ImportError:
            print("PyYAML not installed. Install with: pip install pyyaml")
            return {}
        except Exception as e:
            print(f"Error parsing YAML metadata from {file_path}: {e}")
            return {}
    
    def parse_python_metadata(self, file_path: Path) -> Dict[str, Any]:
        """Parse metadata from Python file with docstring or comments"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            metadata = {
                'description': '',
                'owner': '',
                'tags': [],
                'columns': [],
                'materialization': {},
                'depends': []
            }
            
            # Extract docstring
            docstring_match = re.search(r'"""(.+?)"""', content, re.DOTALL)
            if docstring_match:
                metadata['description'] = docstring_match.group(1).strip()
            else:
                # Try single-line comment at top
                comment_match = re.search(r'^#\s*(.+)', content, re.MULTILINE)
                if comment_match:
                    metadata['description'] = comment_match.group(1).strip()
            
            return metadata
        except Exception as e:
            print(f"Error parsing Python metadata from {file_path}: {e}")
            return {}
    
    def get_asset_metadata(self, asset_file: Path) -> Dict[str, Any]:
        """Get metadata for any asset file"""
        if asset_file.suffix == '.sql':
            return self.parse_sql_metadata(asset_file)
        elif asset_file.suffix in ['.yml', '.yaml']:
            return self.parse_yaml_metadata(asset_file)
        elif asset_file.suffix == '.py':
            return self.parse_python_metadata(asset_file)
        return {}
    
    def calculate_impact_analysis(self, graph: Dict[str, Any], asset_id: str) -> Dict[str, Any]:
        """Calculate impact analysis for a specific asset"""
        
        def get_all_downstream(node_id: str, visited: set = None) -> List[str]:
            """Recursively get all downstream dependencies"""
            if visited is None:
                visited = set()
            if node_id in visited:
                return []
            visited.add(node_id)
            
            downstream = []
            for edge in graph['edges']:
                if edge['source'] == node_id:
                    target = edge['target']
                    downstream.append(target)
                    downstream.extend(get_all_downstream(target, visited))
            
            return list(set(downstream))
        
        def get_all_upstream(node_id: str, visited: set = None) -> List[str]:
            """Recursively get all upstream dependencies"""
            if visited is None:
                visited = set()
            if node_id in visited:
                return []
            visited.add(node_id)
            
            upstream = []
            for edge in graph['edges']:
                if edge['target'] == node_id:
                    source = edge['source']
                    upstream.append(source)
                    upstream.extend(get_all_upstream(source, visited))
            
            return list(set(upstream))
        
        # Get direct dependencies
        direct_downstream = [e['target'] for e in graph['edges'] if e['source'] == asset_id]
        direct_upstream = [e['source'] for e in graph['edges'] if e['target'] == asset_id]
        
        # Get all dependencies
        all_downstream = get_all_downstream(asset_id)
        all_upstream = get_all_upstream(asset_id)
        
        # Calculate criticality score (0-10)
        # Based on: number of downstream assets, presence in reports layer
        downstream_count = len(all_downstream)
        criticality = min(10, downstream_count * 1.2)
        
        # Boost criticality if it affects reports
        reports = [n for n in all_downstream if n.startswith('reports.')]
        if reports:
            criticality = min(10, criticality + len(reports) * 0.5)
        
        # Categorize downstream by layer
        downstream_by_layer = {}
        for node_id in all_downstream:
            node = next((n for n in graph['nodes'] if n['id'] == node_id), None)
            if node:
                layer = node.get('layer', 'unknown')
                if layer not in downstream_by_layer:
                    downstream_by_layer[layer] = []
                downstream_by_layer[layer].append(node_id)
        
        # Determine impact level
        if criticality >= 7:
            impact_level = 'HIGH'
        elif criticality >= 4:
            impact_level = 'MEDIUM'
        else:
            impact_level = 'LOW'
        
        # Estimate rebuild time (rough estimate: 30s per asset)
        estimated_rebuild_seconds = len(all_downstream) * 30
        
        return {
            'asset_id': asset_id,
            'criticality_score': round(criticality, 1),
            'impact_level': impact_level,
            'direct_upstream': direct_upstream,
            'direct_downstream': direct_downstream,
            'total_upstream': all_upstream,
            'total_downstream': all_downstream,
            'downstream_by_layer': downstream_by_layer,
            'affected_reports': reports,
            'estimated_rebuild_seconds': estimated_rebuild_seconds,
            'summary': {
                'total_affected': len(all_downstream),
                'reports_affected': len(reports),
                'layers_affected': len(downstream_by_layer)
            }
        }
    
    def export_for_visualization(self, output_file: str = "pipeline_graph.json"):
        """Export complete pipeline data for visualization"""
        graph = self.build_full_pipeline_graph()
        metadata = self.get_pipeline_metadata()
        
        # Pre-calculate impact analysis for all assets
        impact_data = {}
        print("\nCalculating impact analysis for all assets...")
        for node in graph['nodes']:
            asset_id = node['id']
            impact_data[asset_id] = self.calculate_impact_analysis(graph, asset_id)
        
        output = {
            "pipeline": metadata,
            "graph": graph,
            "impact_analysis": impact_data,
            "timestamp": __import__('datetime').datetime.now().isoformat()
        }
        
        with open(output_file, 'w') as f:
            json.dump(output, f, indent=2)
        
        print(f"\nExported pipeline graph to {output_file}")
        print(f"Total nodes: {len(graph['nodes'])}")
        print(f"Total edges: {len(graph['edges'])}")
        print(f"Layers: {', '.join(graph['stats']['layers'])}")
        print(f"Impact analysis calculated for {len(impact_data)} assets")
        
        return output


def main():
    """Example usage"""
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python bruin_parser.py <path_to_pipeline>")
        print("Example: python bruin_parser.py cohorts/2026/05-data-platforms/nyc-taxi/pipeline")
        sys.exit(1)
    
    pipeline_path = sys.argv[1]
    
    parser = BruinParser(pipeline_path)
    parser.export_for_visualization()


if __name__ == "__main__":
    main()
