import os
import sys
import pandas as pd

def append_csv_files(filepaths, output_file, selected_columns):
    combined_data = pd.DataFrame()

    for file in filepaths:
        if not os.path.exists(file):
            print(f"Error: File {file} does not exist. Skipping...")
            continue

        try:
            # Read the file and select only the required columns
            df = pd.read_csv(file, usecols=selected_columns)
            combined_data = pd.concat([combined_data, df], ignore_index=True)
        except ValueError as e:
            print(f"Error processing {file}: {e}")
            continue

    # Ensure the output directory exists
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    # Save the combined DataFrame to the output file
    combined_data.to_csv(output_file, index=False)
    print(f"Appended results saved to: {output_file}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python append_csv.py <file1> <file2> ...")
        sys.exit(1)

    # Filepaths passed as command-line arguments
    filepaths = sys.argv[1:]

    # Define the output file path
    output_csv = "./result_all_scenarios/appended_results.csv"

    # Specify the columns to include
    columns_to_include = ["class_name", "slo_value", "availability", "scenario"]

    append_csv_files(filepaths, output_csv, columns_to_include)
