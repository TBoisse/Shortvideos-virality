import pandas as pd
import os

data_path = './data/data.csv'
channel_path = './data/channel_data.csv'
output_path = './data/raw_data.csv'

print(f"Chargement de {data_path}...")
try:
    # data.csv est apparemment séparé par des tabulations
    df_data = pd.read_csv(data_path, sep='\t')
    
    # fallback au format comma si une seule colonne est détectée par erreur
    if len(df_data.columns) <= 1:
        df_data = pd.read_csv(data_path)
except Exception as e:
    print(f"Avertissement lors de la lecture avec tabulation : {e}")
    df_data = pd.read_csv(data_path)
    
print(f"Chargement de {channel_path}...")
df_channel = pd.read_csv(channel_path)

print("Fusion des données...")
# data.csv contient 'channelId', channel_data.csv contient 'channel_id'
df_merged = pd.merge(df_data, df_channel, left_on='channelId', right_on='channel_id', how='left')

print(f"Sauvegarde dans {output_path}...")
df_merged.to_csv(output_path, index=False)

print(f"Fusion terminée avec succès ! Le fichier contient {len(df_merged)} lignes.")