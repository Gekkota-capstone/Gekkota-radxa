import os
import time
import boto3
import cv2
from datetime import datetime, timedelta
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from dotenv import load_dotenv

# 환경 설정
load_dotenv()

RECORD_PATH = "/home/radxa/Videos"
S3_BUCKET = "direp"
S3_VIDEO_FOLDER = "stream/"
S3_IMAGE_FOLDER = "opencv/"
MOTION_THRESHOLD = 5000

AWS_REGION = os.getenv("AWS_DEFAULT_REGION")
s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=AWS_REGION
)

def upload_image_to_s3(image_path):
    try:
        image_name = os.path.basename(image_path)
        s3.upload_file(image_path, S3_BUCKET, os.path.join(S3_IMAGE_FOLDER, image_name))
        print(f"🖼️ 업로드됨: s3://{S3_BUCKET}/{S3_IMAGE_FOLDER}{image_name}")
        os.remove(image_path)
    except Exception as e:
        print(f"❌ 이미지 업로드 실패: {image_path} - {e}")

def extract_and_upload_keyframes(video_path, creation_time_str):
    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        print(f"❌ 영상 열기 실패: {video_path}")
        return

    fps = int(cap.get(cv2.CAP_PROP_FPS))
    base_time = datetime.strptime(creation_time_str, "%Y%m%d_%H%M%S")

    ret, prev_frame = cap.read()
    if not ret:
        cap.release()
        return

    frame_id = 0
    while True:
        ret, frame = cap.read()
        if not ret:
            break

        if frame_id % fps == 0:
            gray_prev = cv2.cvtColor(prev_frame, cv2.COLOR_BGR2GRAY)
            gray_current = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
            diff = cv2.absdiff(gray_prev, gray_current)
            _, thresh = cv2.threshold(diff, 30, 255, cv2.THRESH_BINARY)
            score = cv2.countNonZero(thresh)

            if score > MOTION_THRESHOLD:
                seconds = frame_id // fps
                timestamp = base_time + timedelta(seconds=seconds)
                filename = f"record_{timestamp.strftime('%Y-%m-%d-%H-%M-%S')}.jpg"
                image_path = os.path.join("/tmp", filename)
                cv2.imwrite(image_path, frame)
                upload_image_to_s3(image_path)

            prev_frame = frame

        frame_id += 1

    cap.release()

class VideoHandler(FileSystemEventHandler):
    def on_moved(self, event):
        if not event.dest_path.endswith(".mp4"):
            return

        video_file = os.path.basename(event.dest_path)
        print(f"📦 업로드 대상: {video_file}")

        try:
            base = os.path.splitext(video_file)[0]
            time_str = base.replace("record_", "")
            extract_and_upload_keyframes(event.dest_path, time_str)

            s3.upload_file(event.dest_path, S3_BUCKET, os.path.join(S3_VIDEO_FOLDER, video_file))
            print(f"✅ 업로드 완료: s3://{S3_BUCKET}/{S3_VIDEO_FOLDER}{video_file}")
            os.remove(event.dest_path)
            print(f"🗑️ 삭제 완료: {video_file}")
        except Exception as e:
            print(f"❌ 처리 실패: {video_file} - {e}")

if __name__ == "__main__":
    print("📡 영상 감시 및 업로드 시작...")
    observer = Observer()
    observer.schedule(VideoHandler(), path=RECORD_PATH, recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
