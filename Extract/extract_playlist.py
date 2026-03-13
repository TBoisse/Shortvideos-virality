from googleapiclient.discovery import build
import json
import datetime

import pandas as pd

df = pd.read_csv("data.csv", sep=";")
channel_ids = df["channelId"].unique()

with open("key.txt", "r") as f:
    API_KEY = f.readline().strip()

youtube = build("youtube", "v3", developerKey=API_KEY)

def get_upload_playlist(channel_id):
    return "UUSH" + channel_id[2:]


def fetch_all_videos(channel_id):

    playlist_id = get_upload_playlist(channel_id)

    page_token = None
    videos = []

    while True:
        response = youtube.playlistItems().list(
            part="snippet,contentDetails",
            playlistId=playlist_id,
            maxResults=50,
            pageToken=page_token
        ).execute()

        video_ids = [item["contentDetails"]["videoId"] for item in response["items"]]


        videos_request = youtube.videos().list(
            part="snippet,statistics,contentDetails,topicDetails",
            id=",".join(video_ids)
        )
        videos_response = videos_request.execute()

        for item in videos_response["items"]:
            snippet = item["snippet"]
            stats = item.get("statistics", {})
            details = item["contentDetails"]
            topic = item.get("topicDetails", {})

            video_data = {
                "id": item["id"],
                "title": snippet["title"],
                "description": snippet["description"],
                "publishedAt": snippet["publishedAt"],
                "duration": details["duration"],

                "channelId": snippet["channelId"],
                "tags": snippet.get("tags", []),
                "topics": topic.get("topicIds", []),

                "views": stats.get("viewCount"),
                "likes": stats.get("likeCount"),
                "comments": stats.get("commentCount"),

                "thumbnails": snippet["thumbnails"]["default"]["url"]
            }

            videos.append(video_data)

        page_token = response.get("nextPageToken")

        if not page_token:
            break
    
    with open(f"jsons/output_{channel_id}.json", "w+") as f:
        now = datetime.datetime.today()
        now_utc = now.astimezone(datetime.timezone.utc)
        zulu_time = now_utc.isoformat().replace("+00:00", "Z")
        full_data = {"date": zulu_time, "videos" : videos}
        data = json.dumps(full_data)
        f.write(data)
    return videos

for id in channel_ids[10:100]:
    fetch_all_videos(id)