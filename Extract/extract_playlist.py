from Extract.extract_video import extract_data_from_ids

def get_upload_playlist(channel_id):
    return "UUSH" + channel_id[2:]

def extract_data_from_channel(youtube, channel_id):
    playlist_id = get_upload_playlist(channel_id)
    page_token = None
    video_ids = []
    while True:
        response = youtube.playlistItems().list(
            part="snippet,contentDetails",
            playlistId=playlist_id,
            maxResults=50,
            pageToken=page_token
        ).execute()
        video_ids += [item["contentDetails"]["videoId"] for item in response["items"]]
        page_token = response.get("nextPageToken")
        if not page_token:
            break
    return extract_data_from_ids(youtube, video_ids, f"jsons/output_{channel_id}.json")
