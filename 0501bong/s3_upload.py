# s3_upload.py

import os
import time
import cv2
import shutil
import requests
from datetime import datetime, timedelta
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

load_dotenv()
RECORD_PATH = "/home/radxa/Videos"
FRAME_PATH = "/home/radxa/Frames"
API_BASE_URL = "https://api.saffir.co.kr"

upload_executor = ThreadPoolExecutor(max_workers=2)
extract_executor = ProcessPoolExecutor(max_workers=2)


def load_sn():
    try:
        if os.path.exists("sn.txt"):
            with open("sn.txt", "r") as f:
                return f.read().strip()
        elif os.path.exists("/home/radxa/sn.txt"):
            with open("/home/radxa/sn.txt", "r") as f:
                return f.read().strip()
        else:
            return "UNKNOWN"
    except:
        return "UNKNOWN"


def get_presigned_opencv_url(sn, filename):
    try:
        url = f"{API_BASE_URL}/s3/opencv/upload-url"
        payload = {"SN": sn, "filename": filename}
        res = requests.post(url, json=payload, timeout=10)
        res.raise_for_status()
        return res.json().get("upload_url")
    except:
        return None


def get_presigned_video_url(sn, filename):
    try:
        url = f"{API_BASE_URL}/s3/stream/upload-url"
        payload = {"SN": sn, "filename": filename}
        res = requests.post(url, json=payload, timeout=10)
        res.raise_for_status()
        return res.json().get("upload_url")
    except:
        return None


def upload_and_remove_image(image_path):
    try:
        image_name = os.path.basename(image_path)
        sn = load_sn()
        if not sn:
            if os.path.exists(image_path):
                os.remove(image_path)
            return
        presigned_url = get_presigned_opencv_url(sn, image_name)
        if not presigned_url:
            if os.path.exists(image_path):
                os.remove(image_path)
            return
        with open(image_path, "rb") as f:
            res = requests.put(
                presigned_url, data=f, headers={"Content-Type": "image/jpeg"}
            )
            if res.status_code != 200:
                return
    except:
        pass
    finally:
        if os.path.exists(image_path):
            try:
                os.remove(image_path)
            except:
                pass


def upload_video_to_s3(video_path):
    try:
        video_file = os.path.basename(video_path)
        sn = load_sn()
        if not sn:
            return
        time.sleep(1)
        if not os.path.exists(video_path):
            return
        presigned_url = get_presigned_video_url(sn, video_file)
        if not presigned_url:
            return
        with open(video_path, "rb") as f:
            res = requests.put(
                presigned_url, data=f, headers={"Content-Type": "video/mp4"}
            )
            if res.status_code == 200:
                os.remove(video_path)
    except:
        pass


def process_frames_for_video(video_filename):
    try:
        base_name = os.path.splitext(video_filename)[0]
        sn, ts = base_name.split("_", 1)
        base_time = datetime.strptime(ts, "%Y%m%d_%H%M%S")
        prev_frame = None
        delete_queue = []
        upload_queue = []
        all_frames = []
        for filename in os.listdir(FRAME_PATH):
            if filename.startswith(f"{sn}_") and filename.endswith(".jpg"):
                try:
                    frame_time_str = filename.split("_", 1)[1].split(".")[0]
                    frame_time = datetime.strptime(frame_time_str, "%Y%m%d_%H%M%S")
                    if base_time <= frame_time < base_time + timedelta(seconds=60):
                        all_frames.append((filename, frame_time))
                except:
                    continue
        all_frames.sort(key=lambda x: x[1])
        if all_frames:
            first_frame_path = os.path.join(FRAME_PATH, all_frames[0][0])
            try:
                prev_frame = cv2.imread(first_frame_path, cv2.IMREAD_GRAYSCALE)
                delete_queue.append(first_frame_path)
            except:
                delete_queue.append(first_frame_path)
        for i in range(1, len(all_frames)):
            frame_name, _ = all_frames[i]
            frame_path = os.path.join(FRAME_PATH, frame_name)
            try:
                current_frame = cv2.imread(frame_path, cv2.IMREAD_GRAYSCALE)
                if current_frame is None:
                    delete_queue.append(frame_path)
                    continue
                if prev_frame is not None:
                    diff = cv2.absdiff(prev_frame, current_frame)
                    _, thresh = cv2.threshold(diff, 30, 255, cv2.THRESH_BINARY)
                    score = cv2.countNonZero(thresh)
                    if score > 5000:
                        upload_queue.append(frame_path)
                    else:
                        delete_queue.append(frame_path)
                else:
                    delete_queue.append(frame_path)
                prev_frame = current_frame
            except:
                delete_queue.append(frame_path)
        for frame_path in upload_queue:
            upload_executor.submit(upload_and_remove_image, frame_path)
        time.sleep(2)
        for frame_path in delete_queue:
            if os.path.exists(frame_path):
                try:
                    os.remove(frame_path)
                except:
                    pass
    except:
        pass


class VideoHandler(FileSystemEventHandler):
    def __init__(self):
        self.processed_files = set()

    def on_moved(self, event):
        if not event.dest_path.endswith(".mp4"):
            return
        video_file = os.path.basename(event.dest_path)
        if video_file.startswith("temp_"):
            return
        if video_file in self.processed_files:
            return
        self.processed_files.add(video_file)
        try:
            time.sleep(1)
            upload_executor.submit(upload_video_to_s3, event.dest_path)
            upload_executor.submit(process_frames_for_video, video_file)
        except:
            pass

    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith(".mp4"):
            video_file = os.path.basename(event.src_path)
            if (
                not video_file.startswith("temp_")
                and video_file not in self.processed_files
            ):
                self.processed_files.add(video_file)
                time.sleep(1)
                upload_executor.submit(upload_video_to_s3, event.src_path)
                upload_executor.submit(process_frames_for_video, video_file)


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
