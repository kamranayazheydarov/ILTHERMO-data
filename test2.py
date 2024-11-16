import pandas as pd

df = pd.read_csv('density_data1.csv')
df_smiles = pd.read_csv('compounds.csv')

compound_id_1 = df['compound id 1']
smiles = df_smiles['smiles']

comp1smiles = []
for i in compound_id_1:
        smile = df_smiles.loc[df_smiles['id'] == i, 'smiles'].values[0]
        comp1smiles.append(smile)

# Create a DataFrame with two columns: 'comp1id' and 'comp1-smiles'
fd = pd.DataFrame({
    'comp1id': compound_id_1,

})
fd = pd.DataFrame({
    'comp1-smiles': comp1smiles
})

print(fd)


