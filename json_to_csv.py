import json
import csv
import os

# Function to convert JSON to CSV
def json_to_csv(json_file, csv_file, additional_data):
    all_data = []
    headers = None
    
    with open(json_file, 'r') as jf:
        data = json.load(jf)
    
    # Extracting the relevant data from the JSON structure
    if 'data' in data and isinstance(data['data'], list):
        data_list = data['data']
        if headers is None:
            headers = ['setid'] + [header[0] for header in data['dhead']] + list(additional_data.keys())
        
        # Clean the data to ensure only numbers are written to the CSV
        for row in data_list:
            idset_value = f'{os.path.basename(json_file)[-10:-5]}'
            cleaned_row = [idset_value]
            for item in row:
                if isinstance(item, list):
                    cleaned_row.append(item[0])
                else:
                    cleaned_row.append(item)
            # Add additional data columns
            cleaned_row.extend([additional_data[col].get(idset_value, '') for col in additional_data])
            all_data.append(cleaned_row)
    else:
        print(f"Unexpected JSON structure: {json.dumps(data, indent=2)}")
        raise ValueError("JSON data does not contain the expected 'data' list")
    
    # Write all data to a single CSV file
    with open(csv_file, 'w', newline='') as cf:
        writer = csv.writer(cf)
        writer.writerow(headers)
        writer.writerows(all_data)

# Load additional data from output.csv
def load_additional_data(csv_file):
    additional_data = {}
    with open(csv_file, 'r') as cf:
        reader = csv.DictReader(cf)
        for row in reader:
            setid = row['setid']
            for key, value in row.items():
                if key != 'setid':
                    if key not in additional_data:
                        additional_data[key] = {}
                    additional_data[key][setid] = value
    return additional_data

# Process each JSON file in the density_data folder and save to a separate CSV file
density_data_folder = 'meltingtemp_json_data'
output_folder = 'meltingtemp_csv_data'
additional_data_file = '/home/narmak/Desktop/ilthermo/meltpoint-output.csv'

os.makedirs(output_folder, exist_ok=True)

additional_data = load_additional_data(additional_data_file)

json_files = [os.path.join(density_data_folder, filename) for filename in os.listdir(density_data_folder) if filename.endswith('.json')]
for json_file in json_files:
    output_csv_file = os.path.join(output_folder, f"{os.path.splitext(os.path.basename(json_file))[0]}.csv")
    json_to_csv(json_file, output_csv_file, additional_data)
