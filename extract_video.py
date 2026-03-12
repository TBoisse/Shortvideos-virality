from googleapiclient.discovery import build
import json
from itertools import islice
import datetime

def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

with open("key.txt", "r") as f:
    API_KEY = f.readline().strip()

youtube = build("youtube", "v3", developerKey=API_KEY)

# 1. Recherche des vidéos
video_ids = []
next_page = None
number_req = 4
for _ in range(number_req):
    search_request = youtube.search().list(
        part="snippet",
        q="#shorts",
        type="video",
        # order="date",
        maxResults=50,
        pageToken=next_page
    )

    search_response = search_request.execute()
    for item in search_response["items"]:
        video_ids.append(item["id"]["videoId"])
    next_page = search_response.get("nextPageToken")
    if not next_page:
        break

videos = []
for chunk in chunks(video_ids, 50):
    videos_request = youtube.videos().list(
        part="snippet,statistics,contentDetails,topicDetails",
        id=",".join(chunk)
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

with open("output.json", "w+") as f:
    now = datetime.datetime.today()
    now_utc = now.astimezone(datetime.timezone.utc)
    zulu_time = now_utc.isoformat().replace("+00:00", "Z")
    full_data = {"date": zulu_time, "videos" : videos}
    data = json.dumps(full_data)
    f.write(data)