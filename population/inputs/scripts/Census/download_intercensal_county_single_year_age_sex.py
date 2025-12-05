import requests
import os

import pandas as pd
from urllib.error import HTTPError

def download_csv_files(base_url, start_num, end_num, filename_pattern, save_directory):
    """
    Download a series of CSV files with sequential URLs

    Args:
        base_url: Base URL pattern (e.g., "https://example.com/data_{}.csv")
        start_num: Starting number in sequence
        end_num: Ending number in sequence
        filename_pattern: Local filename pattern (e.g., "file_{}.csv")
        save_directory: Directory to save files
    """
    # Create directory if it doesn't exist
    os.makedirs(save_directory, exist_ok=True)

    for i in range(start_num, end_num + 1):
        # Format URL with current number
        url = base_url + filename_pattern.format(str(i).zfill(2))  # Zero-pad if necessary

        # Download the file
        try:
            df = pd.read_csv(filepath_or_buffer=url)
        except HTTPError as e:
            print(f"Failed to download {url}: {e}")
            continue

        # Save the file
        filename = filename_pattern.format(str(i).zfill(2))
        filepath = os.path.join(save_directory, filename)

        df.to_csv(filepath, index=False)

        print(f"Downloaded: {filename}")



# Example usage:
if __name__ == "__main__":
    # Modify these parameters for your specific use case
    base_url = "https://www2.census.gov/programs-surveys/popest/datasets/2020-2024/counties/asrh/"
    start_number = 1
    end_number = 56
    filename_pattern = "cc-est2024-syasex-{}.csv"
    save_directory = r"D:\OneDrive\ICLUS_v3\population\inputs\raw_files\Census\2024\intercensal\syasex"

    download_csv_files(base_url, start_number, end_number, filename_pattern, save_directory)