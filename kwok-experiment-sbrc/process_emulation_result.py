import pandas as pd
import sys
import os

def process_pod_data(file_path):
    # Load the data
    data = pd.read_csv(file_path)

    # Extract the pod group identifier by removing the hash part
    data['pod_group'] = data['pod'].str.extract(r'^((gold|bronze|silver)-class-deploy-\d+)-')[0]

    # Remove rows where pod_group is NaN (pods that don't conform to the pattern)
    data = data.dropna(subset=['pod_group'])

    # Filter and calculate running entries and total entries
    running_entries = data[(data['value'] == 1) & (data['phase'] == 'Running')].groupby('pod_group').size()
    total_entries = data[data['value'] == 1].groupby('pod_group').size()

    # Combine the results into a DataFrame
    result = pd.DataFrame({
        'running_entries': running_entries,
        'total_entries': total_entries
    }).reset_index()

    # Fill missing groups with zeros
    result = result.fillna({'running_entries': 0, 'total_entries': 0}).astype({
        'running_entries': 'int64',
        'total_entries': 'int64'
    })

    # Add the additional required columns
    result['availability'] = (result['running_entries'] / result['total_entries'].replace(0, float('nan'))) * 100
    result['class_name'] = result['pod_group'].str.extract(r'^(gold|silver|bronze)', expand=False)

    # Define SLO values based on class
    slo_mapping = {'gold': 100, 'silver': 90, 'bronze': 50}
    result['slo_value'] = result['class_name'].map(slo_mapping)

    # Add the scenario column
    result['scenario'] = 'emulation'

    # Rename columns as per the requirements
    result.rename(columns={'pod_group': 'pod_name'}, inplace=True)

    # Save the final result to a new CSV file
    output_dir = './result-emulation'
    os.makedirs(output_dir, exist_ok=True)
    final_result_file_path = os.path.join(output_dir, 'final_pod_data_processed.csv')
    result.to_csv(final_result_file_path, index=False)

    return final_result_file_path

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python process_emulation_result.py <file_path>")
        sys.exit(1)

    filepath = sys.argv[1]

    if not os.path.exists(filepath):
        print(f"Error: File {filepath} does not exist.")
        sys.exit(1)

    try:
        output_file = process_pod_data(filepath)
        print(f"Processed data saved to: {output_file}")
    except Exception as e:
        print(f"An error occurred during processing: {e}")
        sys.exit(1)
