import os
import sys
import json
import zipfile
import polars as pl
from dotenv import load_dotenv
from kaggle.api.kaggle_api_extended import KaggleApi
import contextlib

load_dotenv()

def extract_and_stream():
    # 1. Initialize Kaggle API
    # Ensure your kaggle.json is in ~/.kaggle/ or your .env is set
    api = KaggleApi()
    api.authenticate()

    # Create tmp directory if it doesn't exist
    os.makedirs("./tmp", exist_ok=True)

    dataset = "olistbr/brazilian-ecommerce"

    # Download the entire dataset
    print("Downloading entire dataset...", file=sys.stderr)
    with contextlib.redirect_stdout(open(os.devnull, 'w')):
        api.dataset_download_files(dataset, path="./tmp", unzip=True)
    
    # List all CSV files in tmp
    files = [f for f in os.listdir("./tmp") if f.endswith('.csv')]
    
    # Print all files found
    print(f"Found {len(files)} CSV files in tmp:", file=sys.stderr)
    for f in sorted(files):
        print(f"  - {f}", file=sys.stderr)

    for idx, file_name in enumerate(files, 1):
        print(f"[{idx}/{len(files)}] Processing {file_name}...", file=sys.stderr)
        try:
            # Files are already downloaded and unzipped
            file_path = f"./tmp/{file_name}"
            
            # 3. Read with Polars (Super fast)
            df = pl.read_csv(file_path)

            # 4. Send SCHEMA message
            schema = {"properties": {}}
            key_properties = []
            for col, dtype in df.schema.items():
                if dtype == pl.String:
                    schema["properties"][col] = {"type": "string"}
                elif dtype in [pl.Int64, pl.Int32]:
                    schema["properties"][col] = {"type": "integer"}
                elif dtype in [pl.Float64, pl.Float32]:
                    schema["properties"][col] = {"type": "number"}
                elif dtype == pl.Boolean:
                    schema["properties"][col] = {"type": "boolean"}
                else:
                    schema["properties"][col] = {"type": "string"}  # fallback
                if "id" in col.lower() or col.lower().endswith("_id"):
                    key_properties.append(col)
            
            schema_message = {
                "type": "SCHEMA",
                "stream": file_name.replace(".csv", ""),
                "schema": schema,
                "key_properties": key_properties
            }
            try:
                sys.stdout.write(json.dumps(schema_message) + "\n")
            except BrokenPipeError:
                sys.exit(0)

            # 5. Stream to Standard Out (STDOUT) in JSONL format
            # This is what Meltano "listens" to
            for row in df.to_dicts():
                # We add a 'type' and 'stream' key so Meltano knows which table it is
                message = {
                    "type": "RECORD",
                    "stream": file_name.replace(".csv", ""),
                    "record": row
                }
                try:
                    sys.stdout.write(json.dumps(message) + "\n")
                except BrokenPipeError:
                    # Pipe closed, exit gracefully
                    sys.exit(0)
            
            # Clean up the temp files
            os.remove(file_path)
        except Exception as e:
            print(f"Error processing {file_name}: {e}", file=sys.stderr)
            continue

if __name__ == "__main__":
    extract_and_stream()