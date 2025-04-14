import pandas as pd
import sys

# Receive the file path from the command line
if len(sys.argv) < 2:
    print("Usage: python calculate_avg_availability.py <file_path>")
    sys.exit(1)

file_path = sys.argv[1]

# Load the dataset
data = pd.read_csv(file_path)

# Group by class_name and calculate the average availability
class_avg_availability = data.groupby('class_name')['availability'].mean()

# Display the results
print("Average availability by class:")
print(class_avg_availability)