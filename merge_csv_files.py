import os
import pandas as pd
from tqdm import tqdm

def merge_csv_files(folder_path, output_file='meltingtemp-data.csv', chunksize=10000):
    csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]
    required_columns = [
        'setid', 'Normal melting temperature, K',
        'reference', 'propertiy', 'phases', 'compound id 1', 'smile 1', 'compound name 1',
        'compound id 2', 'smile 2', 'compound name 2', 'compound id 3', 'smile 3', 'compound name 3'
    ]
    
    # First pass to collect all column headers
    for file in csv_files:
        df = pd.read_csv(os.path.join(folder_path, file), nrows=0)
        all_columns = set(df.columns).intersection(required_columns)
    
    all_columns = list(all_columns)
    
    with open(output_file, 'w') as outfile:
        for i, file in enumerate(tqdm(csv_files, desc="Processing CSV files")):
            try:
                for chunk in pd.read_csv(os.path.join(folder_path, file), chunksize=chunksize, on_bad_lines='skip'):
                    chunk = chunk[[col for col in required_columns if col in chunk.columns]]  # Keep only required columns that exist
                    chunk.to_csv(outfile, index=False, header=(i == 0 and chunk.index[0] == 0))
            except pd.errors.ParserError as e:
                print(f"Error parsing {file}: {e}")
    
    try:
        return pd.read_csv(output_file, on_bad_lines='skip')
    except pd.errors.ParserError as e:
        print(f"Error reading merged file: {e}")
        return None

merge_csv_files('meltingtemp_csv_data')

