import os
import time
import boto3
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from dotenv import load_dotenv

# ✅ CPU 고정: 0,1,2
try:
    os.sched_setaffinity(0, {0, 1, 2})
except AttributeError:
    pass

load_dotenv()
RECORD_PATH = "/home/radxa/Videos"
S3_BUCKET = "direp"
S3_VIDEO_FOLDER = "stream/"
AWS_REGION = os.getenv("AWS_DEFAULT_REGION")

s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=AWS_REGION
)

class VideoHandler(FileSystemEventHandler):
    def __init__(self):
        self.processed_files = set()

    def on_moved(self, event):
        if not event.dest_path.endswith(".mp4") or event.dest_path.endswith(".done.mp4"):
            return

        video_file = os.path.basename(event.dest_path)
        if video_file in self.processed_files:
            return
        self.processed_files.add(video_file)

        try:
            s3.upload_file(event.dest_path, S3_BUCKET, os.path.join(S3_VIDEO_FOLDER, video_file))
            done_path = event.dest_path.replace(".mp4", ".done.mp4")
            os.rename(event.dest_path, done_path)
        except:
            pass

if __name__ == "__main__":
    observer = Observer()
    observer.schedule(VideoHandler(), path=RECORD_PATH, recursive=False)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
