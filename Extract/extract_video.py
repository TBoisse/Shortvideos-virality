import json
import datetime
import PrintUtils

getby_list = ["date","rating","relevance","videoCount","viewCount"]
maxresults = 50
searchlist_quota = 100

def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def get_video_ids_scratch(youtube, number_req, queries="#shorts", getby="relevance"):
    assert getby in getby_list, PrintUtils.logError(f"*-* Get video ids scratch : Error getby ({getby}) not in getby list !", True)
    PrintUtils.logInfo(f"*-* Get video ids scratch : Getting {number_req * maxresults} videos [{number_req * searchlist_quota} quotas]")
    video_ids = []
    next_page = None
    for i in range(number_req):
        search_request = youtube.search().list(
            part="snippet",
            q=queries,
            type="video",
            order=getby,
            maxResults=maxresults,
            pageToken=next_page
        )
        search_response = search_request.execute()
        for item in search_response["items"]:
            video_ids.append(item["id"]["videoId"])
        next_page = search_response.get("nextPageToken")
        if not next_page:
            PrintUtils.logDebug(f"*-* End of page before number of request {i} / {number_req}.")
            break
    return video_ids


def extract_data_from_ids(youtube, video_ids, output_json_path = "output.json"):
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

    with open(output_json_path, "w+") as f:
        now = datetime.datetime.today()
        now_utc = now.astimezone(datetime.timezone.utc)
        zulu_time = now_utc.isoformat().replace("+00:00", "Z")
        full_data = {"date": zulu_time, "videos" : videos}
        data = json.dumps(full_data)
        f.write(data)

    return True
