import requests
import json
import os
from tqdm import tqdm
""" 
prop_name |prop_id | length | 
-----------------------------
Density   |JkYu    | 8770 | 
Refractive index | bNnk | 2184 | 
Melting point | NmYB | 1262 |
"""
dens = ['density','JkYu']
refindex = ['refracrive-index','bNnk']
meltingtemp = ['melting-temperature','NmYB']

url_for_get_all = 'https://ilthermo.boulder.nist.gov/ILT2/ilsearch?cmp=&ncmp=0&year=&auth=&keyw=&prp=' # +query
url_data_for_setid = 'https://ilthermo.boulder.nist.gov/ILT2/ilset?set=' # +setid

def get_setid_list(list): # e.g input -> dens = []
    url = f"{url_for_get_all}{list[1]}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        with open(f'idsets/{list[0]}-idset.json', 'w') as json_file:
            json.dump(data, json_file)
    else:
        print(f"Failed to retrieve data: {response.status_code}")
density_setids = []
refindex_setids = []
meltingtemp_setids = []

def read_idsets_and_combine():
    for filename in os.listdir('idsets'):
        if filename == 'density-idset.json':
            with open(os.path.join('idsets', filename), 'r') as json_file:
                data = json.load(json_file)
                for i in range(len(data['res'])):
                    x = data['res'][i][0]
                    density_setids.append(x)

        if filename == 'refracrive-index-idset.json':
            with open(os.path.join('idsets', filename), 'r') as json_file:
                data = json.load(json_file)
                for i in range(len(data['res'])):
                    x = data['res'][i][0]
                    refindex_setids.append(x)

        if filename == 'melting-temperature-idset.json':
            with open(os.path.join('idsets', filename), 'r') as json_file:
                data = json.load(json_file)
                for i in range(len(data['res'])):
                    x = data['res'][i][0]
                    meltingtemp_setids.append(x)


if not os.path.exists('idsets'):
    os.makedirs('idsets')

read_idsets_and_combine()

def get_folder_size(folder):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(folder):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
    return total_size

def fetch_and_save_data(filename,setids, folder_name, start_index=0):
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)
    
    total = len(setids)
    for setid in tqdm(setids[start_index:], desc=f"Downloading {folder_name} data", unit="file"):
        url = f"{url_data_for_setid}{setid}"
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            with open(f'{folder_name}/{filename}_setid_{setid}.json', 'w') as json_file:
                json.dump(data, json_file)
            folder_size = get_folder_size(folder_name)
            tqdm.write(f"Current {folder_name} folder size: {folder_size / (1024 * 1024):.2f} MB for {setid}")
        except requests.exceptions.RequestException as e:
            print(f"Failed to retrieve data for setid {setid}: {e}")

read_idsets_and_combine()
filenames = ['density','refindex','meltingtemp']
#fetch_and_save_data(filenames[0],density_setids, 'density_data', start_index=1610)
fetch_and_save_data(filenames[1],refindex_setids, 'refindex_data')
fetch_and_save_data(filenames[2],meltingtemp_setids, 'meltingtemp_data')
