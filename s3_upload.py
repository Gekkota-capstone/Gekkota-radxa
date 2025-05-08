import os
import time
import cv2
import shutil
import requests
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

RECORD_PATH = "/home/radxa/Videos"
FRAME_PATH = "/home/radxa/Frames"
API_BASE_URL = "https://api.saffir.co.kr"

# 이미지 업로드용 스레드 풀
upload_executor = ThreadPoolExecutor(max_workers=5)

# 이미 처리한 파일을 추적하기 위한 세트
processed_files = set()
processed_videos = set()

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
    except Exception:
        return "UNKNOWN"

def get_presigned_opencv_url(sn, filename):
    try:
        url = f"{API_BASE_URL}/s3/opencv/upload-url"
        payload = {"SN": sn, "filename": filename}
        res = requests.post(url, json=payload, timeout=10)
        res.raise_for_status()
        return res.json().get("upload_url")
    except Exception:
        return None

def get_presigned_video_url(sn, filename):
    try:
        url = f"{API_BASE_URL}/s3/stream/upload-url"
        payload = {"SN": sn, "filename": filename}
        res = requests.post(url, json=payload, timeout=10)
        res.raise_for_status()
        return res.json().get("upload_url")
    except Exception:
        return None

# 이미지 업로드 함수
def upload_and_remove_image(image_path):
    if not os.path.exists(image_path):
        return

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
    except Exception:
        pass
    finally:
        # 항상 로컬 파일 삭제
        if os.path.exists(image_path):
            try:
                os.remove(image_path)
            except Exception:
                pass

def upload_video_to_s3(video_path):
    if not os.path.exists(video_path):
        return

    try:
        video_file = os.path.basename(video_path)
        sn = load_sn()
        if not sn:
            return

        # 파일이 완전히 쓰여지도록 대기
        time.sleep(1)

        presigned_url = get_presigned_video_url(sn, video_file)
        if not presigned_url:
            return

        with open(video_path, "rb") as f:
            res = requests.put(
                presigned_url, data=f, headers={"Content-Type": "video/mp4"}
            )
            if res.status_code == 200:
                os.remove(video_path)
    except Exception:
        pass

# 프레임 폴더 주기적 스캔 및 처리
def scan_frame_directory():
    try:
        files = [f for f in os.listdir(FRAME_PATH) if f.endswith('.jpg')]
        new_files = [f for f in files if f not in processed_files]

        for filename in new_files:
            # 숫자.jpg 형식은 처리하지 않음 (GStreamer 임시 파일)
            if filename.split('.')[0].isdigit():
                processed_files.add(filename)
                continue

            # SFRXC12515GF00001_20250508_003014.jpg 형식 처리
            if '_' in filename:
                parts = filename.split('_')
                if len(parts) >= 2 and len(parts[0]) > 5:  # SN은 일반적으로 5자 이상
                    file_path = os.path.join(FRAME_PATH, filename)
                    upload_executor.submit(upload_and_remove_image, file_path)
                    processed_files.add(filename)
    except Exception:
        pass

# 비디오 폴더 주기적 스캔 및 처리
def scan_video_directory():
    try:
        files = [f for f in os.listdir(RECORD_PATH) if f.endswith('.mp4') and not f.startswith('temp_')]
        new_files = [f for f in files if f not in processed_videos]

        for filename in new_files:
            file_path = os.path.join(RECORD_PATH, filename)
            upload_executor.submit(upload_video_to_s3, file_path)
            processed_videos.add(filename)
    except Exception:
        pass

# 메인 실행 함수
if __name__ == "__main__":
    try:
        # 시작 전 폴더 상태 확인
        scan_frame_directory()
        scan_video_directory()

        # 주기적으로 폴더 스캔
        while True:
            scan_frame_directory()
            scan_video_directory()
            time.sleep(0.5)  # 0.5초마다 스캔
    except KeyboardInterrupt:
        pass
