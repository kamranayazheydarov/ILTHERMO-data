import os
import json
import pandas as pd
from tqdm import tqdm
import logging
from multiprocessing import Pool, cpu_count
import csv

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def read_density_id_set(column=None):
    """Reads the density ID set JSON file and optionally extracts a specific column."""
    try:
        with open('idsets/density-idset.json', 'r') as file:
            data = json.load(file)
        if column:
            return [item[column] for item in data if column in item]
        return data
    except Exception as e:
        logging.error(f"Error reading density ID set: {e}")
        return []

def read_json_file(filepath, column=None):
    """Reads a JSON file from a given filepath and optionally extracts a specific column."""
    try:
        with open(filepath, 'r') as file:
            data = json.load(file)
        if column:
            return [item.get(column, None) for item in data]
        return data
    except Exception as e:
        logging.error(f"Error reading JSON file {filepath}: {e}")
        return {}
        
def clean_numeric_value(value):
    """Clean numeric values by extracting only the number."""
    if pd.isna(value):
        return None
    try:
        value_str = str(value)
        # Remove all non-numeric characters except decimal point and minus sign
        import re
        matches = re.findall(r'-?\d+\.?\d*', value_str)
        return float(matches[0]) if matches else None
    except (ValueError, TypeError):
        return None

def clean_temperature_value(value):
    """Clean temperature values by extracting only the number."""
    if isinstance(value, str):
        # Remove all non-numeric characters except decimal point and minus sign
        import re
        matches = re.findall(r'-?\d+\.?\d*', value)
        if matches:
            return matches[0]
    return value

def get_smiles_for_compound_ids(compound_ids, compounds_csv_path):
    """Get SMILES strings for a list of compound IDs using the compounds CSV file."""
    df_smiles = pd.read_csv(compounds_csv_path)
    smiles_list = []
    for compound_id in compound_ids:
        smile = df_smiles.loc[df_smiles['id'] == compound_id, 'smiles']
        if not smile.empty:
            smiles_list.append(smile.values[0])
        else:
            smiles_list.append(None)
    return smiles_list

def process_json_files_to_csv(json_files, output_file, column_mappings, valid_set_ids=None, smiles_mapping=None):
    """Processes a list of JSON files, merges their data, and saves to a CSV file."""
    merged_data = []
    merged_header = None

    # Define all required columns
    required_columns = [
        'setid', 
        'Temperature, K',
        'Pressure, kPa', 
        'Specific density, kg/m³',
        'reference',
        'property',
        'phases',
        'compound id 1',
        'compound name 1',
        'smile 1',
        'compound id 2', 
        'compound name 2',
        'smile 2',
        'compound id 3',
        'compound name 3',
        'smile 3'
    ]

    for json_file in tqdm(json_files, desc=f"Processing JSON files for {output_file}"):
        set_id = os.path.splitext(json_file)[0].split('_')[-1]
        
        # Skip if setid is not in valid_setids
        if valid_set_ids and set_id not in valid_set_ids:
            continue
            
        full_data = read_json_file(os.path.join('density_data', json_file))
        
        header = ['setid'] + [column_mappings.get(item[0], item[0]) for item in full_data.get('dhead', [])]

        data = []
        for row in full_data.get('data', []):
            cleaned_row = [set_id]
            for val, item in zip(row, full_data.get('dhead', [])):
                column_name = column_mappings.get(item[0], item[0])
                if column_name == 'Temperature, K':
                    cleaned_val = clean_temperature_value(val)
                elif column_name == 'Pressure, kPa':
                    cleaned_val = clean_temperature_value(val)
                elif isinstance(val, list) and len(val) == 2:
                    cleaned_val = f"{val[0]}±{val[1]}"
                else:
                    cleaned_val = val
                cleaned_row.append(cleaned_val)

            data.append(cleaned_row)
        
        if merged_header is None:
            merged_header = header
        else:
            for col in header:
                if col not in merged_header:
                    merged_header.append(col)
            
            for row in data:
                while len(row) < len(merged_header):
                    row.append(None)

        merged_data.extend(data)
    
    df_merged = pd.DataFrame(merged_data, columns=merged_header)
    
    # Clean numeric values for temperature and pressure

    
    # Rename the specific density column
    df_merged.rename(columns={'Specific density, kg/m<SUP>3</SUP>': 'Specific density, kg/m³'}, inplace=True)
    
    # Add missing columns with None values
    for col in required_columns:
        if col not in df_merged.columns:
            df_merged[col] = None
    
    # Ensure Temperature and Pressure columns are not null
    df_merged['Temperature, K'] = df_merged['Temperature, K'].apply(lambda x: clean_numeric_value(x) if pd.notna(x) else None)
    df_merged['Pressure, kPa'] = df_merged['Pressure, kPa'].apply(lambda x: clean_numeric_value(x) if pd.notna(x) else None)
    
    # Ensure all JSON files provide values for the specified four columns
    for col in ['Temperature, K', 'Pressure, kPa', 'Specific density, kg/m³', 'reference']:
        if col not in df_merged.columns:
            df_merged[col] = None
    
    # Add SMILES columns based on compound IDs
 
    
    # Keep only the required columns
    df_filtered = df_merged[required_columns]
    df_filtered = df_filtered.where(pd.notnull(df_filtered), None)
    df_filtered.to_csv(output_file, index=False)

def update_density_csv_with_metadata(output_csv_path, density_data_csv_path):
    """
    Update density_data CSV files with reference and other metadata from output.csv,
    matching by setid.
    """
    try:
        # Read output.csv into a dictionary for faster lookups
        output_df = pd.read_csv(output_csv_path)
        output_dict = output_df.set_index('setid').to_dict('index')
        
        # Read the density data file with string dtypes for text columns
        dtype_dict = {
            'setid': str,
            'reference': str,
            'property': str,
            'phases': str,
            'compound id 1': str,
            'compound id 2': str,
            'compound id 3': str,
            'compound name 1': str,
            'compound name 2': str,
            'compound name 3': str
        }
        density_df = pd.read_csv(density_data_csv_path, dtype=dtype_dict)
        
        # Update density data with metadata from output.csv
        for index, row in density_df.iterrows():
            set_id = row['setid']
            if set_id in output_dict:
                metadata = output_dict[set_id]
                # Update each column with proper type handling
                for col, value in metadata.items():
                    if col in density_df.columns:
                        if pd.isna(value):
                            density_df.at[index, col] = None
                        else:
                            density_df.at[index, col] = str(value) if col in dtype_dict else value
        
        # Save updated density data

        try:
            df_smiles = pd.read_csv('compounds.csv')
            compound_id_1 = density_df['compound id 1']
            compound_id_2 = density_df['compound id 2']
            compound_id_3 = density_df['compound id 3']

            comp1smiles = []
            for i in compound_id_1:
                smile = df_smiles.loc[df_smiles['id'] == i, 'smiles']
                if not smile.empty:
                    comp1smiles.append(smile.values[0])
                else:
                    comp1smiles.append(None)
            density_df['smile 1'] = comp1smiles


            comp2smiles = []
            for i in compound_id_2:
                smile = df_smiles.loc[df_smiles['id'] == i, 'smiles']
                if not smile.empty:
                    comp2smiles.append(smile.values[0])
                else:
                    comp2smiles.append(None)

            density_df['smile 2'] = comp2smiles

            comp3smiles = []
            for i in compound_id_3:
                smile = df_smiles.loc[df_smiles['id'] == i, 'smiles']
                if not smile.empty:
                    comp3smiles.append(smile.values[0])
                else:
                    comp3smiles.append(None)

            density_df['smile 3'] = comp3smiles
            density_df['smile 1'] = comp1smiles
        except Exception as m:
            logging.error(f'{m}')
        
        density_df.to_csv(density_data_csv_path, index=False)

    except Exception as e:
        logging.error(f"Error updating {density_data_csv_path} with data from output.csv: {e}")

def create_smiles_dataframe(compounds_csv_path):
    """Create a DataFrame from the compounds CSV file containing 'id' and 'smiles' columns."""
    try:
        df = pd.read_csv(compounds_csv_path, usecols=['id', 'smiles'])
        return df
    except Exception as e:
        logging.error(f"Error creating DataFrame from compounds CSV file: {e}")
        return pd.DataFrame()


def main():
    try:
        # Get valid setids from output.csv
        valid_set_ids = set()
        try:
            df_output = pd.read_csv('output.csv')
            valid_set_ids = set(df_output['setid'].astype(str))
        except Exception as e:
            logging.error(f"Error reading output.csv: {e}")
            return

        # Read all JSON files in the density_data directory
        density_data_dir = 'density_data'
        json_files = [f for f in os.listdir(density_data_dir) if f.endswith('.json')]

        # Split the JSON files into ten parts
        tenth = len(json_files) // 10
        json_files_parts = [json_files[i*tenth:(i+1)*tenth] for i in range(10)]

        column_mappings = {
            'ref': 'reference',
            'prp': 'property',
            'cmp1': 'compound id 1',
            'cmp2': 'compound id 2',
            'cmp3': 'compound id 3',
            'nm1': 'compound name 1',
            'nm2': 'compound name 2',
            'nm3': 'compound name 3'
        }

        # Use multiprocessing to process the JSON files in parallel with valid_setids and smiles_mapping
        with Pool(cpu_count()) as pool:
            pool.starmap(process_json_files_to_csv, 
                [(part, f'density_data{i+1}.csv', column_mappings, valid_set_ids) 
                 for i, part in enumerate(json_files_parts)])

        # Update all density_data files with metadata from output.csv
        for i in range(1, 11):  # Process all 10 density_data files
            density_data_path = f'density_data{i}.csv'
            if os.path.exists(density_data_path):
                update_density_csv_with_metadata('output.csv', density_data_path)
                logging.info(f"Updated {density_data_path} with metadata from output.csv")

    except Exception as e:
        logging.error(f"Error processing density data files: {e}")



if __name__ == "__main__":
    main()