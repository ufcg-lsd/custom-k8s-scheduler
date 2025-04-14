import re
import csv
import subprocess
import signal
import time

# File paths
LOG_FILE = "kubectl_verbose.log"
CSV_FILE = "request_times.csv"

# Regex pattern to extract timestamp and request time
pattern = re.compile(r"I\d{4} (\d{2}:\d{2}:\d+) .* GET .*pods.* in (\d+) (milliseconds|microseconds)")

def log_response_time():
    """ Runs kubectl get pod and logs the total response time (including paginated requests). """
    with open(LOG_FILE, "w") as log:
        subprocess.run(["kubectl", "get", "pod", "--v=6"], stdout=log, stderr=subprocess.STDOUT, text=True)

    total_time = 0
    timestamp = None  # To store the first timestamp of the request sequence

    with open(LOG_FILE, "r") as log:
        for line in log:
            match = pattern.search(line)
            if match:
                ts, time_value, unit = match.groups()

                # Store the first timestamp encountered
                if timestamp is None:
                    timestamp = ts

                # Convert microseconds to milliseconds if needed
                time_value = int(time_value) / 1000 if unit == "microseconds" else int(time_value)

                # Accumulate the total response time
                total_time += time_value

    # Save only one entry with the summed response time
    if timestamp and total_time > 0:
        with open(CSV_FILE, "a", newline="") as csvfile:
            csv_writer = csv.writer(csvfile)
            csv_writer.writerow([timestamp, f"{total_time} ms"])
        print(f"Logged total response time: {total_time} ms.")

def handle_signal(signum, frame):
    log_response_time()

# Register SIGUSR1 signal to trigger logging when requested
signal.signal(signal.SIGUSR1, handle_signal)

# Keep script running, waiting for signals
while True:
    time.sleep(1)
