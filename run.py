import argparse
import logging
import json
import sys
import time
import traceback
import os


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
        
        logging.info("Processing placeholder...") # Remove this later
        
        # Simulating a successful write for now
        logging.info("Job end + status: success")

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
    
