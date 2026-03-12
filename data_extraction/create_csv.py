import pandas as pd
import os
import json

jsons_path = "jsons"
json_list = [os.path.join(jsons_path, filename) for filename in os.listdir(jsons_path) if filename.endswith(".json")]

# On va transformer chaque JSON en DataFrame et ajouter la colonne 'date'
dfs = []
for path in json_list:
    with open(path,"r") as f:
        entry = json.load(f)
    date = entry["date"]
    videos = entry["videos"]
    df_videos = pd.DataFrame(videos)
    df_videos["date"] = date
    dfs.append(df_videos)

# Fusion de tous les DataFrames
final_df = pd.concat(dfs, ignore_index=True)
final_df.to_csv("data.csv", sep='\t', index=False)
