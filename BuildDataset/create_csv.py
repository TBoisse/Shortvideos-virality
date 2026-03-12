import pandas as pd
import json

def build_df_from_json(json_list):
    dfs = []
    for path in json_list:
        with open(path,"r") as f:
            entry = json.load(f)
        date = entry["date"]
        videos = entry["videos"]
        df_videos = pd.DataFrame(videos)
        df_videos["date"] = date
        dfs.append(df_videos)
    return pd.concat(dfs, ignore_index=True)
