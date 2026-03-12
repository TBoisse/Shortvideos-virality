import yt_dlp

video_url = "https://www.youtube.com/watch?v=abc123"

ydl_opts = {
    'outtmpl': 'videos/%(id)s.%(ext)s',
    'format': 'mp4'
}

with yt_dlp.YoutubeDL(ydl_opts) as ydl:
    ydl.download([video_url])