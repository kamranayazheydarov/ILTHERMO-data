import json
import pandas as pd

def json_to_csv(json_filepath, csv_filepath):
    with open(json_filepath, 'r') as json_file:
        data = json.load(json_file)
    
    header = data['header']
    rows = data['res']
    
    # Update header names
    header = [
        "setid", "reference", "property", "phases", 
        "compound id 1", "compound id 2", "compound id 3", 
        "np", "compound name 1", "compound name 2", "compound name 3"
    ]
    
    # Extracting the relevant data from the nested structure
    processed_rows = []
    for row in rows:
        processed_row = []
        for item in row:
            if isinstance(item, dict):
                processed_row.append(item.get('value', ''))
            else:
                processed_row.append(item)
        # Ensure the processed row has the same number of columns as the header
        while len(processed_row) < len(header):
            processed_row.append('')
        processed_rows.append(processed_row)
    
    df = pd.DataFrame(processed_rows, columns=header)
    df.to_csv(csv_filepath, index=False)

# Save JSON data as CSV
json_to_csv('idsets/melting-temperature-idset.json', 'meltpoint-output.csv')

