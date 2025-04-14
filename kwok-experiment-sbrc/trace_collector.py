import csv
import requests
import time
import json
import os
import sys
from datetime import datetime, timedelta
import zipfile
import glob

PROMETHEUS_HOST = "http://localhost:30222"
TIMESTAMP_FILE = "last_run_timestamp.txt"

OUTPUT_BASE_DIR = "./trace-collector-output"

# Ensure the base directory exists
os.makedirs(OUTPUT_BASE_DIR, exist_ok=True)

def read_metrics():
    with open('metrics.txt', 'r') as f:
        metrics = [line.strip() for line in f]
    return metrics

def request_metrics(metric, duration, step):
    end_time = int(time.time())  # Current time as end
    start_time = end_time - int(duration.total_seconds())  # Start time based on duration

    response = requests.get(
        f"{PROMETHEUS_HOST}/api/v1/query_range",
        params={
            "query": metric,
            "start": start_time,
            "end": end_time,
            "step": step  # Add step parameter here
        }
    )

    if response.status_code != 200:
        print(f"Failed to fetch metric {metric}: {response.text}")
        return None

    return response

def write_csv(dir, metrics, duration, step):
    csv_files = []  # To track generated CSV files
    for metric in metrics:
        response = request_metrics(metric, duration, step)

        try:
            results = response.json().get("data", {}).get("result", [])
        except json.JSONDecodeError:
            print(f"Failed to decode JSON for metric {metric}")
            continue

        if len(results) == 0:
            print(f"No results for metric {metric}")
            continue

        metric_name = results[0]["metric"].get("__name__", "")
        csv_file_path = os.path.join(dir, f"{metric_name}.csv")
        csv_files.append(csv_file_path)

        with open(csv_file_path, "w") as f:
            writer = csv.writer(f)
            labelnames = list(results[0]["metric"].keys())

            writer.writerow(["name", "timestamp", "value"] + labelnames)

            for result in results:
                for values in result["values"]:
                    timestamp = values[0]
                    value = values[1]
                    row = [result["metric"].get("__name__", ""), timestamp, value]
                    for label in labelnames:
                        x = result["metric"].get(label, "")
                        row.append(x)
                    writer.writerow(row)

    return csv_files

def main():
    if len(sys.argv) < 3:
        print("Usage: python script.py <duration_in_seconds> <step_in_seconds>")
        sys.exit(1)

    try:
        # Parse duration
        duration = timedelta(seconds=int(sys.argv[1]))
        
        # Parse step directly as an integer
        step = int(sys.argv[2])
    except ValueError as e:
        print(f"Invalid input: {e}")
        sys.exit(1)

    metrics = read_metrics()
    now = datetime.now().strftime("%Y-%m-%d-%H:%M:%S")

    dir = os.path.join(OUTPUT_BASE_DIR, f"output_csv_{now}")
    os.mkdir(dir)
    csv_files = write_csv(dir, metrics, duration, step)

    with zipfile.ZipFile(f"{dir}.zip", "w") as zip:
        for file in csv_files:
            zip.write(file, os.path.basename(file))

    # Return the first CSV file if it exists
    if csv_files:
        first_csv_file = csv_files[0]
        return first_csv_file
    else:
        print("No CSV files were generated.")
        sys.exit(1)

if __name__ == "__main__":
    csv_file_path = main()
    print(csv_file_path)  # Print the CSV file path to stdout for capturing
    sys.exit(0)
