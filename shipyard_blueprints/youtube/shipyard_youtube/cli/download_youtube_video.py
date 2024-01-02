import os
from pytube import YouTube


def main():
    video_id = os.environ.get('YOUTUBE_VIDEO_ID')
    download_type = os.environ.get('YOUTUBE_DOWNLOAD_TYPE')
    file_name = os.environ.get('YOUTUBE_FILE_NAME')

    yt = YouTube(f'https://www.youtube.com/watch?v={video_id}')
    if download_type == 'video':
        stream = yt.streams.filter(only_video=True, res="1080p").first()
    elif download_type == 'audio':
        stream = yt.streams.filter(only_audio=True, abr="160kbps").first()

    stream.download(filename=file_name)


if __name__ == '__main__':
    main()
