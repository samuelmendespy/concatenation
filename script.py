import os
import pandas as pd

data_folder = 'Data_analyse'
df = [pd.read_excel(os.path.join(data_folder, file)) for file in os.listdir(data_folder) if file.endswith('.xlsx')]
if df:
    df_principal = pd.concat(df, axis=0)
    df_principal.to_excel(os.path.join(data_folder, 'concatenado.xlsx'), index=False)
    print('Concatenation completed')
else:
    print('No .xlsx files found in the specified folder.')