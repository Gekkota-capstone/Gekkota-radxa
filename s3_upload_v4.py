import os
import time
import cv2
import shutil
import requests
import re
from datetime import datetime, timedelta
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

try:
    os.sched_setaffinity(0, {2, 3})
except AttributeError:
    pass

load_dotenv()
RECORD_PATH = "/home/radxa/Videos"
API_BASE_URL = "https://api.saffir.co.kr"

upload_executor = ThreadPoolExecutor(max_workers=2)
extract_executor = ProcessPoolExecutor(max_workers=2)

def load_sn():
    try:
        with open("sn.txt", "r") as f:
            return f.read().strip()
    except:
        return None

def get_presigned_opencv_url(sn, filename):
    try:
        print(f"📡 [요청] ZIP 업로드용 Pre-signed URL → {filename}")
        url = f"{API_BASE_URL}/s3/opencv/upload-url"
        payload = {"SN": sn, "filename": filename}
        res = requests.post(url, json=payload, timeout=10)
        res.raise_for_status()
        print(f"✅ [응답] ZIP URL 발급 완료")
        return res.json().get("upload_url")
    except Exception as e:
        print(f"❌ [오류] ZIP URL 요청 실패: {e}")
        return None

def get_presigned_video_url(sn, filename):
    try:
        print(f"📡 [요청] 영상 업로드용 Pre-signed URL → {filename}")
        url = f"{API_BASE_URL}/s3/stream/upload-url"
        payload = {"SN": sn, "filename": filename}
        res = requests.post(url, json=payload, timeout=10)
        res.raise_for_status()
        print(f"✅ [응답] 영상 URL 발급 완료")
        return res.json().get("upload_url")
    except Exception as e:
        print(f"❌ [오류] 영상 URL 요청 실패: {e}")
        return None

def extract_and_upload_zip_from_video(video_path, creation_time_str):
    print(f"\n🎞️ [시작] 영상 처리 시작: {video_path}")

    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        print(f"❌ [오류] 영상 열기 실패 (OpenCV)")
        return

    fps = int(cap.get(cv2.CAP_PROP_FPS))
    sn = load_sn()
    if not sn:
        print("❌ [오류] SN 파일 로딩 실패")
        return

    base_time = datetime.strptime(creation_time_str, "%Y%m%d_%H%M%S")
    folder_name = f"{sn}_{creation_time_str}"
    folder_path = os.path.join("/tmp", folder_name)
    os.makedirs(folder_path, exist_ok=True)
    print(f"📁 [폴더 생성] {folder_path}")

    frame_id = 0
    saved_count = 0

    while True:
        ret, frame = cap.read()
        if not ret:
            break

        if frame_id % fps == 0:
            gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
            resized = cv2.resize(gray, (1280, 720))
            seconds = frame_id // fps
            timestamp = base_time + timedelta(seconds=seconds)
            filename = f"{sn}_{timestamp.strftime('%Y-%m-%d-%H-%M-%S')}.jpg"
            image_path = os.path.join(folder_path, filename)
            cv2.imwrite(image_path, resized)
            saved_count += 1

        frame_id += 1

    cap.release()
    print(f"🖼️ [완료] 이미지 저장: {saved_count}장")

    zip_filename = f"{folder_name}.zip"
    zip_path = os.path.join("/tmp", zip_filename)
    shutil.make_archive(zip_path.replace(".zip", ""), 'zip', folder_path)
    print(f"🗜️ [압축] ZIP 파일 생성: {zip_path}")

    presigned_zip_url = get_presigned_opencv_url(sn, zip_filename)
    if presigned_zip_url:
        try:
            with open(zip_path, "rb") as f:
                headers = {"Content-Type": "application/zip"}
                res = requests.put(presigned_zip_url, data=f, headers=headers)
                res.raise_for_status()
                print(f"✅ [업로드] ZIP 업로드 완료")
        except Exception as e:
            print(f"❌ [오류] ZIP 업로드 실패: {e}")
            return

    os.remove(zip_path)
    shutil.rmtree(folder_path)
    print("🧹 [정리] ZIP 파일 및 폴더 삭제")

    video_file = os.path.basename(video_path)
    presigned_video_url = get_presigned_video_url(sn, video_file)
    if presigned_video_url:
        try:
            with open(video_path, "rb") as f:
                headers = {"Content-Type": "video/mp4"}
                res = requests.put(presigned_video_url, data=f, headers=headers)
                res.raise_for_status()
                print(f"✅ [업로드] 영상 업로드 완료")
        except Exception as e:
            print(f"❌ [오류] 영상 업로드 실패: {e}")

    if os.path.exists(video_path):
        os.remove(video_path)
        print(f"🧹 [정리] 영상 삭제 완료: {video_file}")

    print(f"✅ [완료] 영상 처리 전체 완료: {video_file}\n")

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

        match = re.search(r"(\d{8}_\d{6})", video_file)
        if not match:
            print(f"⚠️ [경고] 시간 문자열 추출 실패: {video_file}")
            return

        time_str = match.group(1)
        print(f"\n🛰️ [감지] 영상 이동됨: {video_file}")
        extract_executor.submit(extract_and_upload_zip_from_video, event.dest_path, time_str)

if __name__ == "__main__":
    print("📡 영상 폴더 감시 시작...")
    observer = Observer()
    observer.schedule(VideoHandler(), path=RECORD_PATH, recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
