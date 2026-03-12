# extern
from googleapiclient.discovery import build
import os
import pandas as pd
# intern
from Extract import extract_data_from_channel
from BuildDataset import build_df_from_json
import PrintUtils
# usefull variables
jsons_path = "jsons"
csv_path = "data/data.csv"
key_path = "key.txt"

# ----------- MAIN

PrintUtils.logInfo("*-* Main : Generating youtube object !")
with open(key_path, "r") as f:
    API_KEY = f.readline().strip()
youtube = build("youtube", "v3", developerKey=API_KEY)

PrintUtils.logInfo("*-* Main : Getting all json files !")
df = pd.read_csv(csv_path, sep="\t")
channel_ids = df["channelId"].unique()
pre_json_list = [os.path.join(jsons_path, filename) for filename in os.listdir(jsons_path) if filename.endswith(".json")]
for id in channel_ids[10:100]:
    if os.path.exists(os.path.join(jsons_path, f"output_{id}.json")):
        PrintUtils.logWarning(f"*-* Main : {id} already exists so skipped !")
        continue
    PrintUtils.logInfo(f"*-* Main : {id} not found so extracting channel !")
    extract_data_from_channel(id)

PrintUtils.logInfo("*-* Main : Merging all json files !")
json_list = [os.path.join(jsons_path, filename) for filename in os.listdir(jsons_path) if filename.endswith(".json")]
final_df = build_df_from_json(json_list)
final_df.to_csv(csv_path, sep='\t', index=False)