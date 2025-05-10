import os
import time
import cv2
import shutil
import requests
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import threading

RECORD_PATH = "/home/radxa/Videos"
FRAME_PATH = "/home/radxa/Frames"
API_BASE_URL = "https://api.saffir.co.kr"

# 이미지와 영상 업로드용 스레드 풀 각각 생성
image_upload_executor = ThreadPoolExecutor(max_workers=2)

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
            print("❌ SN 파일을 찾을 수 없음")
            return "UNKNOWN"
    except Exception as e:
        print(f"❌ SN 로드 실패: {e}")
        return "UNKNOWN"


def get_presigned_opencv_url(sn, filename):
    try:
        print(f"📡 jpg URL 요청 중: {filename}")
        url = f"{API_BASE_URL}/s3/opencv/upload-url"
        payload = {"SN": sn, "filename": filename}
        res = requests.post(url, json=payload, timeout=10)
        res.raise_for_status()
        print(f"✅ jpg URL 발급 성공: {filename}")
        return res.json().get("upload_url")
    except Exception as e:
        print(f"❌ jpg URL 요청 실패: {filename} - {e}")
        return None


def get_presigned_video_url(sn, filename):
    try:
        print(f"📡 영상 URL 요청 중: {filename}")
        url = f"{API_BASE_URL}/s3/stream/upload-url"
        payload = {"SN": sn, "filename": filename}
        res = requests.post(url, json=payload, timeout=10)
        res.raise_for_status()
        print(f"✅ 영상 URL 발급 성공: {filename}")
        return res.json().get("upload_url")
    except Exception as e:
        print(f"❌ 영상 URL 요청 실패: {filename} - {e}")
        return None


def upload_and_remove_image(image_path):
    if not os.path.exists(image_path):
        print(f"❌ 이미지 파일 없음: {image_path}")
        return

    try:
        image_name = os.path.basename(image_path)
        sn = load_sn()
        if not sn:
            print(f"❌ SN 로드 실패, 이미지 삭제: {image_name}")
            if os.path.exists(image_path):
                os.remove(image_path)
            return

        presigned_url = get_presigned_opencv_url(sn, image_name)
        if not presigned_url:
            print(f"❌ URL 발급 실패, 이미지 삭제: {image_name}")
            if os.path.exists(image_path):
                os.remove(image_path)
            return

        with open(image_path, "rb") as f:
            print(f"📤 이미지 업로드 시작: {image_name}")
            res = requests.put(
                presigned_url, data=f, headers={"Content-Type": "image/jpeg"}
            )
            if res.status_code == 200:
                print(f"✅ 이미지 업로드 성공: {image_name}")
            else:
                print(f"❌ 이미지 업로드 실패: {image_name}, 상태 코드: {res.status_code}")
    except Exception as e:
        print(f"❌ 이미지 업로드 오류: {image_path} - {e}")
    finally:
        if os.path.exists(image_path):
            try:
                os.remove(image_path)
                print(f"🗑️ 이미지 삭제 완료: {os.path.basename(image_path)}")
            except Exception as e:
                print(f"❌ 이미지 삭제 실패: {image_path} - {e}")


def upload_video_to_s3(video_path):
    if not os.path.exists(video_path):
        print(f"❌ 영상 파일 없음: {video_path}")
        return

    try:
        video_file = os.path.basename(video_path)
        print(f"📤 영상 업로드 시작: {video_path}")
        sn = load_sn()
        if not sn:
            print("❌ SN 로드 실패")
            return

        time.sleep(1)

        presigned_url = get_presigned_video_url(sn, video_file)
        if not presigned_url:
            print(f"❌ 영상 URL 발급 실패: {video_file}")
            return

        with open(video_path, "rb") as f:
            res = requests.put(
                presigned_url, data=f, headers={"Content-Type": "video/mp4"}
            )
            if res.status_code == 200:
                print(f"✅ 영상 업로드 성공: {video_file}")
                os.remove(video_path)
                print(f"🗑️ 영상 삭제 완료: {video_file}")
            else:
                print(f"❌ 영상 업로드 실패: {video_file}, 상태 코드: {res.status_code}")
    except Exception as e:
        print(f"❌ 영상 업로드 오류: {video_path} - {e}")


def scan_frame_directory():
    try:
        files = [f for f in os.listdir(FRAME_PATH) if f.endswith('.jpg')]
        new_files = [f for f in files if f not in processed_files]

        if new_files:
            print(f"🔍 새 프레임 {len(new_files)}개 발견")

        for filename in new_files:
            if filename.split('.')[0].isdigit():
                processed_files.add(filename)
                continue

            if '_' in filename:
                parts = filename.split('_')
                if len(parts) >= 2 and len(parts[0]) > 5:
                    file_path = os.path.join(FRAME_PATH, filename)
                    print(f"🖼️ 프레임 업로드 큐에 추가: {filename}")
                    image_upload_executor.submit(upload_and_remove_image, file_path)
                    processed_files.add(filename)
    except Exception as e:
        print(f"❌ 프레임 폴더 스캔 오류: {e}")


def scan_video_directory():
    try:
        files = [f for f in os.listdir(RECORD_PATH) if f.endswith('.mp4') and not f.startswith('temp_')]
        new_files = [f for f in files if f not in processed_videos]

        if new_files:
            print(f"🔍 새 영상 {len(new_files)}개 발견")

        for filename in new_files:
            file_path = os.path.join(RECORD_PATH, filename)
            print(f"📦 영상 업로드 큐에 추가: {filename}")
            threading.Thread(target=upload_video_to_s3, args=(file_path,), daemon=True).start()
            processed_videos.add(filename)
    except Exception as e:
        print(f"❌ 비디오 폴더 스캔 오류: {e}")



if __name__ == "__main__":
    print("🚀 S3 업로드 서비스 시작...")
    print(f"📂 영상 경로: {RECORD_PATH}")
    print(f"📂 프레임 경로: {FRAME_PATH}")

    try:
        print("🧹 기존 파일 확인 중...")
        scan_frame_directory()
        scan_video_directory()

        print("🔄 주기적 폴더 스캔 시작 (0.5초 간격)")

        while True:
            scan_frame_directory()
            scan_video_directory()
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("👋 종료 신호 받음")
        print("✅ S3 업로드 서비스 종료")
