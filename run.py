import argparse
import logging
import json
import sys
import time
import traceback
import os
import pandas as pd
import yaml
import numpy as np
from pathlib import Path


#We set version="unknown" as a default just in case the script crashes before it even reads config.yaml (e.g., if the user types a bad CLI argument)

def write_error_output(output_path, error_msg, version="unknown"):
    """Writes the structured error JSON if anything fails."""
    error_data = {
        "version": version,
        "status": "error",
        "error_message": str(error_msg)
    }
    # Create the file and write the JSON
    with open(output_path, 'w') as f:
        json.dump(error_data, f, indent=2)
    
    # Also print it to stdout (required for Docker evaluation)
    print(json.dumps(error_data, indent=2))


def main():
    # 1. Setup CLI Arguments
    parser = argparse.ArgumentParser(description="Minimal MLOps batch job")
    parser.add_argument("--input", required=True, help="Path to input data.csv")
    parser.add_argument("--config", required=True, help="Path to config.yaml")
    parser.add_argument("--output", required=True, help="Path to output metrics.json")
    parser.add_argument("--log-file", required=True, help="Path to write run.log")
    
    args = parser.parse_args()

    # 2. Configure Observability (Logging)
    logging.basicConfig(
        filename=args.log_file,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    logging.info("=========================================")
    logging.info("Job start: MLOps Batch Pipeline Initiated")

    # Track start time for latency metrics later
    start_time = time.time()
    config_version = "unknown" # Default until we read the config

    try:
        # ---------------------------------------------------------
        # - Load/Validate config (update config_version)
        # - Load/Validate data
        # - Calculate rolling mean & signals
        # - Write success metrics to args.output
        # ---------------------------------------------------------
        
        # ==========================================
        # 1. LOAD & VALIDATE CONFIG
        # ==========================================
        if not os.path.exists(args.config):
            raise FileNotFoundError(f"Config file not found at {args.config}")
            
        with open(args.config, 'r') as f:
            config = yaml.safe_load(f)
            
        if config is None or not isinstance(config, dict):
            raise ValueError("Config is empty or not a valid YAML dictionary.")
            
        # Check required keys
        required_keys = ['seed', 'window', 'version']
        for key in required_keys:
            if key not in config:
                raise KeyError(f"Missing required config key: '{key}'")
                
        # Update the version 
        config_version = config['version']
        
        # Set deterministic seeds
        np.random.seed(config['seed'])
        
        logging.info(f"Config loaded and validated. Version: {config_version}, Window: {config['window']}, Seed: {config['seed']}")

        # ==========================================
        # 2. LOAD & VALIDATE DATASET
        # ==========================================
        #fix the csv (run once)
        # inp = Path("data.csv - Sheet1.csv")
        # out = Path("data.csv")
        # with inp.open("r", encoding="utf-8") as f_in, out.open("w", encoding="utf-8", newline="") as f_out:
        #     for line in f_in:
        #         line = line.rstrip("\n")
        #         if line.startswith('"') and line.endswith('"'):
        #             line = line[1:-1]
        #         f_out.write(line + "\n")

        if not os.path.exists(args.input):
            raise FileNotFoundError(f"Input file not found at {args.input}")
            
        

        

        # Read the CSV
        try:
            df = pd.read_csv(args.input)
        except Exception as e:
            raise ValueError(f"Failed to parse CSV: {str(e)}")
            
        # Validate dataset structure
        if df.empty:
            raise ValueError("The input dataset is completely empty.")
            
        if 'close' not in df.columns:
            raise KeyError("The required column 'close' is missing from the dataset.")
            

        # Clean the close column (ensure it's numeric)
        df['close'] = pd.to_numeric(df['close'], errors='coerce')
        if df['close'].isna().any():
            logging.warning("Found non-numeric/missing values in 'close'. Forward-filling to maintain row count.")
            df['close'] = df['close'].ffill().bfill() # ffill first, bfill if the very first row is NaN

        if df.empty:
             raise ValueError("After cleaning invalid 'close' values, the dataset is empty.")

        logging.info(f"Data loaded and validated. Rows to process: {len(df)}")
        
        
        # ==========================================
        # 3. ROLLING MEAN
        # ==========================================
        # Calculate the rolling mean.
        df['rolling_mean'] = df['close'].rolling(window=config['window']).mean()
        
        # REQUIREMENT: Define how to handle the first window-1 rows.
        # DECISION: We will backfill (bfill) the NaN values. 
        # This ensures we don't lose rows (keeping rows_processed = 10000)
        # and avoids undefined math behavior when generating the signal.
        df['rolling_mean'] = df['rolling_mean'].bfill()
        
        logging.info("Calculated rolling mean and handled window-1 NaNs via backfill.")

        # ==========================================
        # 4. GENERATE SIGNAL
        # ==========================================
        # signal = 1 if close > rolling_mean else 0
        df['signal'] = (df['close'] > df['rolling_mean']).astype(int)
        
        logging.info("Generated trading signals.")

        # ==========================================
        # 5. METRICS + TIMING
        # ==========================================
        # Calculate metrics
        rows_processed = len(df)
        signal_rate = float(df['signal'].mean())
        
        # Calculate latency
        end_time = time.time()
        latency_ms = int((end_time - start_time) * 1000)

        # Construct output dictionary
        metrics_dict = {
            "version": config_version,
            "rows_processed": rows_processed,
            "metric": "signal_rate",
            "value": round(signal_rate, 4), # Rounded to 4 decimal places 
            "latency_ms": latency_ms,
            "seed": config['seed'],
            "status": "success"
        }

        # Write to JSON file
        with open(args.output, 'w') as f:
            json.dump(metrics_dict, f, indent=2)
            
        # Print to stdout 
        print(json.dumps(metrics_dict, indent=2))
        
        # Simulating a successful write for now
        logging.info(f"Job end + status: success. Signal rate: {signal_rate:.4f}, Latency: {latency_ms}ms")




    except Exception as e:
        # If ANYTHING goes wrong above, we catch it here.
        logging.error(f"Job failed with error: {str(e)}")
        logging.error(traceback.format_exc()) # Logs the full stack trace to the file
        
        # Write the required error JSON
        write_error_output(args.output, str(e), version=config_version)
        
        # Exit with a non-zero code (Required by the Docker prompt)
        sys.exit(1)

# Standard Python boilerplate to run main()
if __name__ == "__main__":
    main()
    
