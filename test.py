import os
import json
import pandas as pd
from tqdm import tqdm
import logging
from multiprocessing import Pool, cpu_count
import csv

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def read_density_idset(column=None):
    """Reads the density ID set JSON file and optionally extracts a specific column."""
    try:
        with open('idsets/density-idset.json', 'r') as file:
            data = json.load(file)
        if column:
            return [item[column] for item in data if column in item]
        return data
    except Exception as e:
        logging.error(f"Error reading density idset: {e}")
        return []

def read_density_file(filepath, column=None):
    """Reads a JSON file from a given filepath and optionally extracts a specific column."""
    try:
        with open(filepath, 'r') as file:
            data = json.load(file)
        if column:
            return [item.get(column, None) for item in data]
        return data
    except Exception as e:
        logging.error(f"Error reading density file {filepath}: {e}")
        return {}

def clean_temperature(value):
    """Clean temperature values by extracting only the number."""
    if isinstance(value, str):
        import re
        matches = re.findall(r'-?\d+\.?\d*', value)
        if matches:
            return matches[0]
    return value

def clean_numeric_value(value):
    """Clean numeric values by extracting only the number."""
    if pd.isna(value):
        return None
    try:
        value_str = str(value)
        import re
        matches = re.findall(r'-?\d+\.?\d*', value_str)
        return float(matches[0]) if matches else None
    except (ValueError, TypeError):
        return None

def get_smiles_mapping(compounds_csv_path):
    """Reads the compounds CSV file and returns a dictionary mapping compound IDs to SMILES."""
    try:
        compounds_df = pd.read_csv(compounds_csv_path)
        print("Headers in compounds.csv:", compounds_df.columns.tolist())
        return dict(zip(compounds_df['id'], compounds_df['smiles']))
    except Exception as e:
        logging.error(f"Error reading compounds CSV file: {e}")
        return {}

def get_smiles_from_compound_ids(compound_ids, compounds_csv_path):
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

def process_json_files(json_files, output_file, column_mappings, valid_setids=None, smiles_mapping=None):
    """Processes a list of JSON files, merges their data, and saves to a CSV file."""
    merged_data = []
    merged_header = None

    required_columns = [
        'setid', 
        'Temperature, K',
        'Pressure, kPa', 
        'Specific density, kg/m³',
        'reference',
        'propertiy',
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
        setid = os.path.splitext(json_file)[0].split('_')[-1]
        
        if valid_setids and setid not in valid_setids:
            continue
            
        full_data = read_density_file(os.path.join('density_data', json_file))
        
        header = ['setid'] + [column_mappings.get(item[0], item[0]) for item in full_data.get('dhead', [])]
        data = [[setid] + [clean_temperature(val) if column_mappings.get(item[0]) == 'Temperature, K' else 
                          (f"{val[0]}±{val[1]}" if isinstance(val, list) and len(val) == 2 else val)
                          for val, item in zip(row, full_data.get('dhead', []))
                          if not isinstance(val, list) or len(val) != 0]
                for row in full_data.get('data', [])]
        
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
    
    numeric_columns = ['Temperature, K', 'Pressure, kPa']
    for col in numeric_columns:
        if col in df_merged.columns:
            df_merged[col] = df_merged[col].apply(clean_numeric_value)
    
    df_merged.rename(columns={'Specific density, kg/m<SUP>3</SUP>': 'Specific density, kg/m³'}, inplace=True)
    
    for col in required_columns:
        if col not in df_merged.columns:
            df_merged[col] = None
    
    df_merged['Temperature, K'] = df_merged['Temperature, K'].apply(lambda x: clean_numeric_value(x) if pd.notna(x) else None)
    df_merged['Pressure, kPa'] = df_merged['Pressure, kPa'].apply(lambda x: clean_numeric_value(x) if pd.notna(x) else None)
    
    for col in ['Temperature, K', 'Pressure, kPa', 'Specific density, kg/m³', 'reference']:
        if col not in df_merged.columns:
            df_merged[col] = None
    
    if smiles_mapping:
        df_merged['smile 1'] = get_smiles_from_compound_ids(df_merged['compound id 1'], 'compounds.csv')
        print(df_merged['smile 1'])
    
    df_filtered = df_merged[required_columns]
    df_filtered = df_filtered.where(pd.notnull(df_filtered), None)
    df_filtered.to_csv(output_file, index=False)

def update_output_csv(output_csv_path, density_data_csv_path):
    """Update density_data CSV files with reference and other metadata from output.csv, matching by setid."""
    try:
        output_df = pd.read_csv(output_csv_path)
        output_dict = output_df.set_index('setid').to_dict('index')
        
        dtype_dict = {
            'setid': str,
            'reference': str,
            'propertiy': str,
            'phases': str,
            'compound id 1': str,
            'compound id 2': str,
            'compound id 3': str,
            'compound name 1': str,
            'compound name 2': str,
            'compound name 3': str
        }
        density_df = pd.read_csv(density_data_csv_path, dtype=dtype_dict)
        
        for index, row in density_df.iterrows():
            setid = row['setid']
            if setid in output_dict:
                metadata = output_dict[setid]
                for col, value in metadata.items():
                    if col in density_df.columns:
                        if pd.isna(value):
                            density_df.at[index, col] = None
                        else:
                            density_df.at[index, col] = str(value) if col in dtype_dict else value
        
        density_df.to_csv(density_data_csv_path, index=False)
        
    except Exception as e:
        logging.error(f"Error updating {density_data_csv_path} with data from output.csv: {e}")

def main():
    """Main function to process density ID sets and JSON files."""
    try:
        full = read_density_idset()

        header = [h.replace('ref', 'reference').replace('prp', 'propertiy')
                    .replace('cmp1', 'compound id 1').replace('cmp2', 'compound id 2').replace('cmp3', 'compound id 3')
                    .replace('nm1', 'compound name 1').replace('nm2', 'compound name 2').replace('nm3', 'compound name 3')
                    for h in full.get('header', [])]

        data = full.get('res', [])

        df = pd.DataFrame(data, columns=header)
        df.to_csv('output.csv', index=False)
    except Exception as e:
        logging.error(f"Error processing density idset: {e}")

    try:
        valid_setids = set()
        try:
            df_output = pd.read_csv('output.csv')
            valid_setids = set(df_output['setid'].astype(str))
        except Exception as e:
            logging.error(f"Error reading output.csv: {e}")
            return

        density_data_dir = 'density_data'
        json_files = [f for f in os.listdir(density_data_dir) if f.endswith('.json')]

        tenth = len(json_files) // 10
        json_files_parts = [json_files[i*tenth:(i+1)*tenth] for i in range(10)]

        column_mappings = {
            'ref': 'reference',
            'prp': 'propertiy',
            'cmp1': 'compound id 1',
            'cmp2': 'compound id 2',
            'cmp3': 'compound id 3',
            'nm1': 'compound name 1',
            'nm2': 'compound name 2',
            'nm3': 'compound name 3'
        }

        smiles_mapping = get_smiles_mapping('compounds.csv')

        with Pool(cpu_count()) as pool:
            pool.starmap(process_json_files, 
                [(part, f'density_data{i+1}.csv', column_mappings, valid_setids, smiles_mapping) 
                 for i, part in enumerate(json_files_parts)])

        for i in range(1, 11):
            density_data_path = f'density_data{i}.csv'
            if os.path.exists(density_data_path):
                update_output_csv('output.csv', density_data_path)
                logging.info(f"Updated {density_data_path} with metadata from output.csv")

    except Exception as e:
        logging.error(f"Error processing density data files: {e}")

if __name__ == "__main__":
    main()

