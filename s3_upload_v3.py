import os
import time
import boto3
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor


# ✅ 환경 설정
load_dotenv()

RECORD_PATH = "/home/radxa/Videos"
FRAME_PATH = "/home/radxa/Frames"
S3_BUCKET = "direp"
S3_VIDEO_FOLDER = "stream/"
S3_IMAGE_FOLDER = "opencv/"
AWS_REGION = os.getenv("AWS_DEFAULT_REGION")

s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=AWS_REGION
)

upload_executor = ThreadPoolExecutor(max_workers=4)

def upload_image_to_s3(image_path):
    try:
        image_name = os.path.basename(image_path)
        s3.upload_file(image_path, S3_BUCKET, os.path.join(S3_IMAGE_FOLDER, image_name))
        os.remove(image_path)
        print(f"✅ 이미지 업로드 완료: {image_name}")
    except Exception as e:
        print(f"❌ 이미지 업로드 실패: {image_path} - {e}")

def upload_video_to_s3(video_path):
    try:
        video_file = os.path.basename(video_path)
        s3.upload_file(video_path, S3_BUCKET, os.path.join(S3_VIDEO_FOLDER, video_file))
        os.remove(video_path)
        print(f"✅ 영상 업로드 완료: {video_file}")
    except Exception as e:
        print(f"❌ 영상 업로드 실패: {video_path} - {e}")

def upload_recent_frames():
    frame_files = sorted(
        [f for f in os.listdir(FRAME_PATH) if f.endswith(".jpg") and not f.startswith("frame_")],
        key=lambda x: os.path.getmtime(os.path.join(FRAME_PATH, x))
    )[-60:]

    for filename in frame_files:
        full_path = os.path.join(FRAME_PATH, filename)
        upload_executor.submit(upload_image_to_s3, full_path)

class VideoHandler(FileSystemEventHandler):
    def __init__(self):
        self.processed_files = set()

    def on_moved(self, event):
        if not event.dest_path.endswith(".mp4"):
            return

        video_file = os.path.basename(event.dest_path)
        if video_file in self.processed_files:
            return
        self.processed_files.add(video_file)

        print(f"📦 감지된 영상: {video_file}")

        try:
            upload_executor.submit(upload_video_to_s3, event.dest_path)
            upload_recent_frames()
        except Exception as e:
            print(f"❌ 처리 실패: {video_file} - {e}")

if __name__ == "__main__":
    print("📡 영상 및 프레임 감시 시작...")
    observer = Observer()
    observer.schedule(VideoHandler(), path=RECORD_PATH, recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
