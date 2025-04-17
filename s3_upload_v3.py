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

load_dotenv()
RECORD_PATH = "/home/radxa/Videos"
FRAME_PATH = "/home/radxa/Frames"  # 추정: 프레임 저장 경로
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
        print(f"📡 [요청] jpg 업로드용 Pre-signed URL → {filename}")
        url = f"{API_BASE_URL}/s3/opencv/upload-url"
        payload = {"SN": sn, "filename": filename}
        res = requests.post(url, json=payload, timeout=10)
        res.raise_for_status()
        print(f"✅ [응답] jpg URL 발급 완료")
        return res.json().get("upload_url")
    except Exception as e:
        print(f"❌ [오류] jpg URL 요청 실패: {e}")
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

# ✅ 이미지 업로드 및 삭제
def upload_and_remove_image(image_path):
    try:
        image_name = os.path.basename(image_path)
        sn = load_sn()
        if not sn:
            print("❌ SN 로드 실패")
            return

        presigned_url = get_presigned_opencv_url(sn, image_name)
        if not presigned_url:
            return

        with open(image_path, "rb") as f:
            res = requests.put(presigned_url, data=f, headers={"Content-Type": "image/jpeg"})
            if res.status_code == 200:
                print(f"✅ 이미지 업로드 완료: {image_name}")
            else:
                print(f"❌ 이미지 업로드 실패: {image_name}, 상태코드: {res.status_code}")
    except Exception as e:
        print(f"❌ 이미지 업로드 실패: {image_path} - {e}")
    finally:
        if os.path.exists(image_path):
            os.remove(image_path)

# ✅ 영상 업로드
def upload_video_to_s3(video_path):
    try:
        video_file = os.path.basename(video_path)
        print(f"📤 영상 업로드 시작: {video_path}")
        sn = load_sn()
        if not sn:
            print("❌ SN 로드 실패")
            return

        presigned_url = get_presigned_video_url(sn, video_file)
        if not presigned_url:
            return

        with open(video_path, "rb") as f:
            res = requests.put(presigned_url, data=f, headers={"Content-Type": "video/mp4"})
            if res.status_code == 200:
                print(f"✅ 영상 업로드 완료: {video_file}")
                os.remove(video_path)
            else:
                print(f"❌ 영상 업로드 실패: {video_file}, 상태코드: {res.status_code}")
    except Exception as e:
        print(f"❌ 영상 업로드 실패: {video_path} - {e}")

# ✅ 프레임 비교 및 정리
def process_frames_for_video(video_filename):
    try:
        base_name = os.path.splitext(video_filename)[0]  # SNR_YYYYMMDD_HHMMSS
        sn, ts = base_name.split("_", 1)
        base_time = datetime.strptime(ts, "%Y%m%d_%H%M%S")

        prev_frame = None
        delete_queue = []

        for i in range(60):
            ts_i = base_time + timedelta(seconds=i)
            fname = f"{sn}_{ts_i.strftime('%Y%m%d_%H%M%S')}.jpg"
            fpath = os.path.join(FRAME_PATH, fname)

            if not os.path.exists(fpath):
                continue

            if prev_frame is None:
                print(f"⏭️ 첫 프레임: {fname} → 생략")
                prev_frame = cv2.imread(fpath, cv2.IMREAD_GRAYSCALE)
                if prev_frame is not None:
                    delete_queue.append(fpath)
                continue

            current_frame = cv2.imread(fpath, cv2.IMREAD_GRAYSCALE)
            if current_frame is None:
                continue

            diff = cv2.absdiff(prev_frame, current_frame)
            _, thresh = cv2.threshold(diff, 30, 255, cv2.THRESH_BINARY)
            score = cv2.countNonZero(thresh)
            print(f"🔍 변화량 확인: {fname} ➔ 변화량={score}")

            if score > 5000:
                upload_executor.submit(upload_and_remove_image, fpath)
            else:
                delete_queue.append(fpath)

            prev_frame = current_frame

        # 🧹 남은 이미지 정리
        time.sleep(3)  # 업로드 겹침 방지
        for f in delete_queue:
            if os.path.exists(f):
                os.remove(f)

    except Exception as e:
        print(f"❌ 프레임 처리 중 오류: {video_filename} - {e}")

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

        print(f"📦 감지된 영상: {video_file}")
        try:
            upload_executor.submit(upload_video_to_s3, event.dest_path)
            upload_executor.submit(process_frames_for_video, video_file)
        except Exception as e:
            print(f"❌ 처리 실패: {video_file} - {e}")

# ✅ 실행 시작
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
