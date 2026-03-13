# Import source files
import sys
import os
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(ROOT_DIR)
# extern
from googleapiclient.discovery import build
# intern
from Extract import get_video_ids_scratch

key_path = "key.txt"
with open(key_path, "r") as f:
    API_KEY = f.readline().strip()
youtube = build("youtube", "v3", developerKey=API_KEY)
ret = get_video_ids_scratch(youtube, 0)
print(ret)