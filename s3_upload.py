import os
import time
import boto3
from dotenv import load_dotenv
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# .env 파일 로드
load_dotenv()

RECORD_PATH = "/home/radxa/Videos"
S3_BUCKET_NAME = "direp"
S3_FOLDER = "stream/"
AWS_REGION = os.getenv("AWS_DEFAULT_REGION")

# boto3 클라이언트 생성
s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=AWS_REGION
)

def get_previous_filename(current_filename):
    try:
        base, ext = os.path.splitext(current_filename)
        prefix, num = base.rsplit("_", 1)
        prev_num = int(num) - 1
        return f"{prefix}_{prev_num:05d}{ext}"
    except Exception:
        return None

class VideoUploadHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory or not event.src_path.endswith(".mp4"):
            return

        current_file = os.path.basename(event.src_path)
        previous_file = get_previous_filename(current_file)

        if not previous_file:
            print(f"⚠️ 이전 파일명을 파악할 수 없음: {current_file}")
            return

        previous_path = os.path.join(RECORD_PATH, previous_file)
        if not os.path.exists(previous_path):
            print(f"⏭️ 이전 파일이 아직 존재하지 않음: {previous_file}")
            return

        print(f"📦 업로드 대상: {previous_file}")
        time.sleep(2)  # 혹시 쓰기 마무리 중일 수 있으므로 잠시 대기

        try:
            s3_key = os.path.join(S3_FOLDER, previous_file)
            s3.upload_file(previous_path, S3_BUCKET_NAME, s3_key)
            print(f"✅ 업로드 완료: s3://{S3_BUCKET_NAME}/{s3_key}")
            os.remove(previous_path)
            print(f"🗑️ 삭제 완료: {previous_file}")
        except Exception as e:
            print(f"❌ 업로드 중 에러 발생: {previous_file} - {e}")

if __name__ == "__main__":
    event_handler = VideoUploadHandler()
    observer = Observer()
    observer.schedule(event_handler, path=RECORD_PATH, recursive=False)
    observer.start()

    print("📡 영상 감시 및 업로드 시작...")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
