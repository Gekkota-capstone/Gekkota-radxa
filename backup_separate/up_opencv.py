import os
import time
import cv2
import boto3
from datetime import datetime, timedelta
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from dotenv import load_dotenv

# ✅ CPU 고정: 2, 3
try:
    os.sched_setaffinity(0, {2, 3})
except AttributeError:
    pass

load_dotenv()
RECORD_PATH = "/home/radxa/Videos"
S3_BUCKET = "direp"
S3_IMAGE_FOLDER = "opencv/"
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
        os.remove(image_path)
    except:
        pass

def extract_and_upload_keyframes(temp_video_path, creation_time_str, original_video_path):
    cap = cv2.VideoCapture(temp_video_path)
    if not cap.isOpened():
        return

    fps = int(cap.get(cv2.CAP_PROP_FPS))
    base_time = datetime.strptime(creation_time_str, "%Y%m%d_%H%M%S")
    frame_id = 0
    save_id = 0

    ret, prev_frame = cap.read()
    if not ret:
        cap.release()
        return

    while True:
        ret, frame = cap.read()
        if not ret:
            break

        if frame_id % fps == 5:
            gray_prev = cv2.cvtColor(prev_frame, cv2.COLOR_BGR2GRAY)
            gray_current = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)

            diff = cv2.absdiff(gray_prev, gray_current)
            _, diff_thresh = cv2.threshold(diff, 30, 255, cv2.THRESH_BINARY)
            motion_score = cv2.countNonZero(diff_thresh)

            if motion_score > 5000:
                seconds = frame_id // fps
                timestamp = base_time + timedelta(seconds=seconds)
                filename = f"record_{timestamp.strftime('%Y-%m-%d-%H-%M-%S')}.jpg"
                image_path = os.path.join("/tmp", filename)
                cv2.imwrite(image_path, frame)
                upload_image_to_s3(image_path)
                save_id += 1

            prev_frame = frame

        frame_id += 1

    cap.release()
    os.remove(temp_video_path)
    os.remove(original_video_path)

class VideoHandler(FileSystemEventHandler):
    def __init__(self):
        self.processed_files = set()

    def on_moved(self, event):
        if not event.dest_path.endswith(".done.mp4"):
            return

        video_file = os.path.basename(event.dest_path)
        if video_file in self.processed_files:
            return
        self.processed_files.add(video_file)

        time_str = video_file.replace("record_", "").replace(".done.mp4", "")

        try:
            temp_video_path = f"/tmp/tmp_{video_file}"
            os.system(f"cp '{event.dest_path}' '{temp_video_path}'")
            extract_and_upload_keyframes(temp_video_path, time_str, event.dest_path)
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
