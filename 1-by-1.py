import pandas as pd


df_smiles = pd.read_csv('compounds.csv')
output_df = pd.read_csv('meltpoint-output.csv')
compound_id_1 = output_df['compound id 1']
compound_id_2 = output_df['compound id 2']
compound_id_3 = output_df['compound id 3']
comp1smiles = []
for i in compound_id_1:
    smile = df_smiles.loc[df_smiles['compound id'] == i, 'smiles']
    if not smile.empty:
        comp1smiles.append(smile.values[0])
    else:
        comp1smiles.append(None)
output_df['smile 1'] = comp1smiles
comp2smiles = []
for i in compound_id_2:
    smile = df_smiles.loc[df_smiles['compound id'] == i, 'smiles']
    if not smile.empty:
        comp2smiles.append(smile.values[0])
    else:
        comp2smiles.append(None)

output_df['smile 2'] = comp2smiles
comp3smiles = []
for i in compound_id_3:
    smile = df_smiles.loc[df_smiles['compound id'] == i, 'smiles']
    if not smile.empty:
        comp3smiles.append(smile.values[0])
    else:
        comp3smiles.append(None)

output_df['smile 3'] = comp3smiles
output_df['smile 1'] = comp1smiles

output_df.to_csv('meltpoint-output.csv', index=False)
