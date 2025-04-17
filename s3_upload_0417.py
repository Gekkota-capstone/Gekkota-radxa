import os
import time
import boto3
import cv2
from datetime import datetime, timedelta
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

# ✅ 디렉토리 보장
os.makedirs(RECORD_PATH, exist_ok=True)
os.makedirs(FRAME_PATH, exist_ok=True)

# ✅ AWS 연결
s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=AWS_REGION
)

upload_executor = ThreadPoolExecutor(max_workers=4)

# ✅ 이미지 업로드 및 삭제
def upload_and_remove_image(image_path):
    try:
        image_name = os.path.basename(image_path)
        s3.upload_file(image_path, S3_BUCKET, os.path.join(S3_IMAGE_FOLDER, image_name))
        print(f"✅ 이미지 업로드 완료: {image_name}")
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
        s3.upload_file(video_path, S3_BUCKET, os.path.join(S3_VIDEO_FOLDER, video_file))
        os.remove(video_path)
        print(f"✅ 영상 업로드 완료: {video_file}")
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

# ✅ 메인 실행
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
