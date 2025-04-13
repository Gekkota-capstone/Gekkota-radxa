import os
import time
import boto3
import cv2
from datetime import datetime, timedelta
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

try:
    os.sched_setaffinity(0, {2, 3})
except AttributeError:
    pass

# ✅ 환경 설정
load_dotenv()

RECORD_PATH = "/home/radxa/Videos"
S3_BUCKET = "direp"
S3_VIDEO_FOLDER = "stream/"
S3_IMAGE_FOLDER = "opencv/"
AWS_REGION = os.getenv("AWS_DEFAULT_REGION")

# ✅ AWS S3 연결
s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=AWS_REGION
)

# ✅ 병렬 처리 풀
upload_executor = ThreadPoolExecutor(max_workers=2)
extract_executor = ProcessPoolExecutor(max_workers=4)

# ✅ S3 업로드 함수
def upload_image_to_s3(image_path):
    try:
        image_name = os.path.basename(image_path)
        s3.upload_file(image_path, S3_BUCKET, os.path.join(S3_IMAGE_FOLDER, image_name))
        os.remove(image_path)
    except Exception as e:
        print(f" 이미지 업로드 실패: {image_path} - {e}")

# ✅ 영상 업로드 함수
def upload_video_to_s3(video_path):
    try:
        video_file = os.path.basename(video_path)
        s3.upload_file(video_path, S3_BUCKET, os.path.join(S3_VIDEO_FOLDER, video_file))
        os.remove(video_path)
    except Exception as e:
        print(f" 영상 업로드 실패: {video_path} - {e}")

# ✅ 1초당 gray → resize → 저장
def extract_and_upload_keyframes(video_path, creation_time_str):
    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        return

    fps = int(cap.get(cv2.CAP_PROP_FPS))
    base_time = datetime.strptime(creation_time_str, "%Y%m%d_%H%M%S")

    frame_id = 0
    while True:
        ret, frame = cap.read()
        if not ret:
            break

        if frame_id % fps == 0:
            gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
            resized = cv2.resize(gray, (1280, 720))  # HD 해상도 리사이즈

            seconds = frame_id // fps
            timestamp = base_time + timedelta(seconds=seconds)
            filename = f"record_{timestamp.strftime('%Y-%m-%d-%H-%M-%S')}.jpg"
            image_path = os.path.join("/tmp", filename)

            cv2.imwrite(image_path, resized)
            upload_executor.submit(upload_image_to_s3, image_path)

        frame_id += 1

    cap.release()

# ✅ 파일 감시 핸들러
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

        base = os.path.splitext(video_file)[0]
        time_str = base.replace("record_", "")

        try:
            extract_executor.submit(extract_and_upload_keyframes, event.dest_path, time_str)
            upload_executor.submit(upload_video_to_s3, event.dest_path)
        except Exception as e:
            print(f" 처리 실패: {video_file} - {e}")

# ✅ 메인 루프
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
