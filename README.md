# ILThermo Data Processing

This project processes JSON files containing density data and merges them into CSV files. It also updates the CSV files with metadata and SMILES strings for compounds.

## main_functions.py

This script contains the main functions for processing the density data.

### Functions

- `read_density_id_set(column=None)`: Reads the density ID set JSON file and optionally extracts a specific column.
- `read_json_file(filepath, column=None)`: Reads a JSON file from a given filepath and optionally extracts a specific column.
- `clean_numeric_value(value)`: Cleans numeric values by extracting only the number.
- `clean_temperature_value(value)`: Cleans temperature values by extracting only the number.
- `get_smiles_for_compound_ids(compound_ids, compounds_csv_path)`: Gets SMILES strings for a list of compound IDs using the compounds CSV file.
- `process_json_files_to_csv(json_files, output_file, column_mappings, valid_set_ids=None, smiles_mapping=None)`: Processes a list of JSON files, merges their data, and saves to a CSV file.
- `update_density_csv_with_metadata(output_csv_path, density_data_csv_path)`: Updates density_data CSV files with reference and other metadata from output.csv, matching by setid.
- `create_smiles_dataframe(compounds_csv_path)`: Creates a DataFrame from the compounds CSV file containing 'id' and 'smiles' columns.
- `main()`: The main function that orchestrates the processing of density data files.

### Usage

1. Ensure you have the required JSON files in the `density_data` directory.
2. Run the script using Python:

## File Descriptions

### #file:compounds.csv

This file contains compound information with the following headers:
- `id` (integer): The unique identifier for each compound.
- `smiles` (string): The SMILES string representation of the compound.

### #file:density_data.csv

This file contains density data with the following headers:
- `setid` (integer): The unique identifier for each data set.
- `temperature` (float): The temperature at which the density was measured.
- `density` (float): The measured density value.
- `compound_id` (integer): The identifier of the compound associated with the density measurement.
