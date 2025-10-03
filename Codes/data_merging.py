import os
import json
import pandas as pd
from datetime import datetime

# --- Configuration ---
# Set the paths to your three folders here.
# Make sure to replace these with the actual paths on your system.
METRIC_1_FOLDER = r"C:\Users\anurj\Desktop\Journey to Greatness\7G Hackathon\operator_metric1_result_mapped_renamed"
METRIC_2_FOLDER = r"C:\Users\anurj\Desktop\Journey to Greatness\7G Hackathon\operator_metric2_result_mapped_renamed"
METRIC_3_FOLDER = r"C:\Users\anurj\Desktop\Journey to Greatness\7G Hackathon\operator_metric3_result_mapped_renamed"

OUTPUT_FILE = 'merged_data.csv'

# --- Functions ---
def process_json_from_folder(folder_path, metric_name):
    """
    Iterates through all JSON files in a given folder, extracts data,
    and consolidates it into a single pandas DataFrame.
    """
    all_data = []

    # Check if the folder exists and is a directory
    if not os.path.isdir(folder_path):
        print(f"Warning: Folder not found at '{folder_path}'. Skipping.")
        return pd.DataFrame() # Return an empty DataFrame

    print(f"Processing folder: {folder_path}...")
    
    # List all files in the directory
    for filename in os.listdir(folder_path):
        if filename.endswith('.json'):
            file_path = os.path.join(folder_path, filename)
            print(f"  - Reading file: {filename}")
            try:
                with open(file_path, 'r') as f:
                    data = json.load(f)
                    
                    # Extract the list of results from the nested structure
                    results = data["data"]["result"]

                    for item in results:
                        datacenter = item["metric"]["datacenter"]
                        instance = item["metric"]["instance"]
                        values = item["values"]
                        
                        for value in values:
                            timestamp = int(value[0])
                            date_time = datetime.fromtimestamp(timestamp)
                            metric_value = float(value[1])
                            
                            # Append the processed row to our list
                            all_data.append({
                                'Datetime(UTC)': date_time.strftime("%Y-%m-%d %H:%M:%S"),
                                'metric': metric_name,
                                'value': metric_value,
                                'datacenter': datacenter,
                                'instance': instance
                            })
            except Exception as e:
                print(f"Error processing file {filename}: {e}")
                continue  # Skip to the next file on error

    return pd.DataFrame(all_data)

def main():
    """
    Main function to orchestrate the data merging process.
    """
    # Create a list of tuples with folder paths and corresponding metric names
    folders_to_process = [
        (METRIC_1_FOLDER, 'metric_1'),
        (METRIC_2_FOLDER, 'metric_2'),
        (METRIC_3_FOLDER, 'metric_3')
    ]

    processed_count = 0
    # Process each folder and save the resulting DataFrame to a separate file
    for folder_path, metric_name in folders_to_process:
        df = process_json_from_folder(folder_path, metric_name)
        
        if df.empty:
            print(f"No data found for {metric_name}. Skipping.")
            continue

        print(f"Finalizing data for {metric_name}...")
        
        # Convert the Datetime column to a proper datetime type for sorting
        df['Datetime(UTC)'] = pd.to_datetime(df['Datetime(UTC)'])

        # Sort the final DataFrame for a clean, chronological output
        df = df.sort_values(by=['Datetime(UTC)', 'instance']).reset_index(drop=True)

        # Define the output filename for the current metric
        output_filename = f"{metric_name}_data.csv"

        # Save the final merged data to a CSV file
        df.to_csv(output_filename, sep=';', index=False)
        
        print(f"\nSuccessfully processed data for {metric_name}.")
        print(f"Data saved to '{output_filename}'.")
        print("------------------------------------------")
        print("Example of the first 5 rows:")
        print(df.head().to_string())
        print("------------------------------------------")
        processed_count += 1

    if processed_count == 0:
        print("No data was processed. Please check your folder paths and file formats.")
    else:
        print(f"\nFinished processing. Created {processed_count} separate CSV files.")


if __name__ == "__main__":
    main()
