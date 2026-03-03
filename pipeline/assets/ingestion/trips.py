"""@bruin
name: ingestion.trips
type: python
image: python:3.11
connection: duckdb-default

materialization:
  type: table
  strategy: create+replace
@bruin"""

import os
import json
import pandas as pd
import requests
from pathlib import Path
import tempfile

def materialize():
    start_date = os.environ["BRUIN_START_DATE"]
    end_date = os.environ["BRUIN_END_DATE"]
    taxi_types = json.loads(os.environ["BRUIN_VARS"]).get("taxi_types", ["yellow"])

    BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"
    
    # Generate list of months between start and end dates
    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date)
    months = pd.date_range(start=start, end=end, freq='MS')
    
    dataframes = []
    
    for taxi_type in taxi_types:
        for month in months:
            year = month.year
            month_num = month.month
            
            # Download from GitHub (same source as dbt)
            csv_gz_filename = f"{taxi_type}_tripdata_{year}-{month_num:02d}.csv.gz"
            url = f"{BASE_URL}/{taxi_type}/{csv_gz_filename}"
            
            try:
                print(f"Downloading {csv_gz_filename}...")
                
                # Download to temporary file
                with tempfile.NamedTemporaryFile(delete=False, suffix='.csv.gz') as tmp_file:
                    response = requests.get(url, stream=True)
                    response.raise_for_status()
                    
                    for chunk in response.iter_content(chunk_size=8192):
                        tmp_file.write(chunk)
                    
                    tmp_path = tmp_file.name
                
                # Read CSV.gz with pandas
                df = pd.read_csv(tmp_path, compression='gzip')
                df['taxi_type'] = taxi_type
                
                # Clean up temp file
                Path(tmp_path).unlink()
                
                # Convert datetime columns to strings to avoid PyArrow issues
                for col in df.columns:
                    if pd.api.types.is_datetime64_any_dtype(df[col]):
                        df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
                    elif pd.api.types.is_object_dtype(df[col]) or pd.api.types.is_string_dtype(df[col]):
                        df[col] = df[col].astype(str).replace('nan', None)
                
                dataframes.append(df)
                print(f"Successfully loaded {len(df)} rows from {csv_gz_filename}")
                
            except Exception as e:
                print(f"Failed to fetch {url}: {e}")
                continue
    
    if not dataframes:
        return pd.DataFrame()
    
    # Concatenate all dataframes
    final_dataframe = pd.concat(dataframes, ignore_index=True)
    
    return final_dataframe
