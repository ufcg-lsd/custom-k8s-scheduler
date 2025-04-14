import pandas as pd
import matplotlib.pyplot as plt
import sys
import os

# Command-line arguments for input and output file paths
if len(sys.argv) < 3:
    print("Usage: python script.py <input_filepath> <output_directory>")
    sys.exit(1)

input_filepath = sys.argv[1]
output_directory = sys.argv[2]

# Ensure the output directory exists
os.makedirs(output_directory, exist_ok=True)

# Define the output file path for filtered data
output_filepath = os.path.join(output_directory, 'filtered_data.csv')

# Load the CSV file
data = pd.read_csv(input_filepath)

# Ensure the 'node' column is treated as a string
data['node'] = data['node'].astype(str)

# Apply filters: resource is 'cpu' or 'memory', and node is 'minikube'
filtered_data = data[
    (data['resource'].isin(['cpu', 'memory'])) &
    (data['node'] == 'minikube')
]

# Save the filtered data to the specified output file path
filtered_data.to_csv(output_filepath, index=False)

# Convert the timestamp column to datetime if not already
filtered_data['timestamp'] = pd.to_datetime(filtered_data['timestamp'], unit='s')

# Group by resource type and timestamp, summing the values
time_series_data = filtered_data.groupby(['resource', 'timestamp'])['value'].sum().unstack(level=0)

# Plot CPU usage over time and save the plot
cpu_plot_path = os.path.join(output_directory, 'cpu_usage_over_time.png')
plt.figure(figsize=(12, 6))
plt.plot(time_series_data.index, time_series_data['cpu'], marker='o', label='CPU')
plt.title('CPU Usage Over Time (Minikube Node)')
plt.xlabel('Timestamp')
plt.ylabel('Total Value (CPU)')
plt.grid(True)
plt.tight_layout()
plt.savefig(cpu_plot_path)
plt.close()

# Plot Memory usage over time and save the plot
memory_plot_path = os.path.join(output_directory, 'memory_usage_over_time.png')
plt.figure(figsize=(12, 6))
plt.plot(time_series_data.index, time_series_data['memory'], marker='o', label='Memory', color='orange')
plt.title('Memory Usage Over Time (Minikube Node)')
plt.xlabel('Timestamp')
plt.ylabel('Total Value (Memory)')
plt.grid(True)
plt.tight_layout()
plt.savefig(memory_plot_path)
plt.close()

# Print output file locations
print(f"Filtered data saved to: {output_filepath}")
print(f"CPU usage plot saved to: {cpu_plot_path}")
print(f"Memory usage plot saved to: {memory_plot_path}")
