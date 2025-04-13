# s3_upload_v3.py

import os
import time
import cv2
import requests
import boto3
from datetime import datetime, timedelta
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

# ✅ CPU 코어 설정
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

# ✅ S3 이미지 업로드용 boto3
s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=AWS_REGION
)

upload_executor = ThreadPoolExecutor(max_workers=2)
extract_executor = ProcessPoolExecutor(max_workers=4)

# ✅ SN 불러오기
def load_sn():
    try:
        with open("sn.txt", "r") as f:
            return f.read().strip()
    except:
        return None

# ✅ Pre-signed URL 요청 함수
def get_presigned_video_url(sn, filename):
    try:
        url = "https://api.saffir.co.kr/s3/stream/upload-url"
        payload = {
            "SN": sn,
            "filename": filename
        }
        res = requests.post(url, json=payload, timeout=10)
        res.raise_for_status()
        return res.json().get("upload_url")
    except Exception as e:
        print(f"❌ Pre-signed URL 요청 실패: {e}")
        return None

# ✅ 이미지 S3 업로드
def upload_image_to_s3(image_path):
    try:
        image_name = os.path.basename(image_path)
        s3.upload_file(image_path, S3_BUCKET, os.path.join(S3_IMAGE_FOLDER, image_name))
        os.remove(image_path)
    except Exception as e:
        print(f" 이미지 업로드 실패: {image_path} - {e}")

# ✅ 영상 업로드 - Pre-signed 방식
def upload_video_to_s3(video_path):
    sn = load_sn()
    if not sn:
        print("❌ SN 파일 없음 → 영상 업로드 생략")
        return

    video_file = os.path.basename(video_path)
    print(f"📡 Pre-signed URL 요청 중: {video_file}")
    presigned_url = get_presigned_video_url(sn, video_file)

    if not presigned_url:
        print("❌ URL 없음 → 생략")
        return

    try:
        with open(video_path, "rb") as f:
            res = requests.put(presigned_url, data=f)
            res.raise_for_status()
            print(f"✅ 업로드 완료: {video_file}")
        os.remove(video_path)
    except Exception as e:
        print(f"❌ 업로드 실패: {e}")

# ✅ 이미지 추출 및 업로드 (1초마다 grayscale → HD resize)
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
            resized = cv2.resize(gray, (1280, 720))

            seconds = frame_id // fps
            timestamp = base_time + timedelta(seconds=seconds)
            filename = f"record_{timestamp.strftime('%Y-%m-%d-%H-%M-%S')}.jpg"
            image_path = os.path.join("/tmp", filename)

            cv2.imwrite(image_path, resized)
            upload_executor.submit(upload_image_to_s3, image_path)

        frame_id += 1

    cap.release()

# ✅ 이벤트 핸들러
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

# ✅ 메인
if __name__ == "__main__":
    print("📡 영상 감시 시작...")
    observer = Observer()
    observer.schedule(VideoHandler(), path=RECORD_PATH, recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
