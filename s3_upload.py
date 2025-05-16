import os
import time
import cv2
import shutil
import requests
import fcntl
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor
import threading
import logging

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

RECORD_PATH = "/home/radxa/Videos"
FRAME_PATH = "/home/radxa/Frames"
API_BASE_URL = "https://api.saffir.co.kr"
LOCK_FILE = "/home/radxa/upload_lock.lock"  # 업로드 동기화용 잠금 파일
UPLOAD_TRACKER = "/home/radxa/Videos/.upload_tracker"  # 업로드 상태 추적 파일

# 이미지와 영상 업로드용 스레드 풀 각각 생성
image_upload_executor = ThreadPoolExecutor(max_workers=2)
video_upload_executor = ThreadPoolExecutor(max_workers=2)  # 영상 업로드용 스레드 풀

# 이미 처리한 파일을 추적하기 위한 세트
processed_files = set()
processed_videos = set()

# 업로드 실패한 파일을 추적하기 위한 딕셔너리 (파일명: 재시도 횟수)
failed_uploads = {}
MAX_RETRY = 3  # 최대 재시도 횟수

# 파일 잠금을 통한 동기화 헬퍼 함수
def with_file_lock(func):
    def wrapper(*args, **kwargs):
        lock_file = open(LOCK_FILE, 'w+')
        try:
            fcntl.flock(lock_file, fcntl.LOCK_EX)
            return func(*args, **kwargs)
        finally:
            fcntl.flock(lock_file, fcntl.LOCK_UN)
            lock_file.close()
    return wrapper


def load_sn():
    try:
        if os.path.exists("sn.txt"):
            with open("sn.txt", "r") as f:
                return f.read().strip()
        elif os.path.exists("/home/radxa/sn.txt"):
            with open("/home/radxa/sn.txt", "r") as f:
                return f.read().strip()
        else:
            logger.error("❌ SN 파일을 찾을 수 없음")
            return "UNKNOWN"
    except Exception as e:
        logger.error(f"❌ SN 로드 실패: {e}")
        return "UNKNOWN"


def get_presigned_opencv_url(sn, filename):
    try:
        logger.info(f"📡 jpg URL 요청 중: {filename}")
        url = f"{API_BASE_URL}/s3/opencv/upload-url"
        payload = {"SN": sn, "filename": filename}
        res = requests.post(url, json=payload, timeout=10)
        res.raise_for_status()
        logger.info(f"✅ jpg URL 발급 성공: {filename}")
        return res.json().get("upload_url")
    except Exception as e:
        logger.error(f"❌ jpg URL 요청 실패: {filename} - {e}")
        return None


def get_presigned_video_url(sn, filename):
    try:
        logger.info(f"📡 영상 URL 요청 중: {filename}")
        url = f"{API_BASE_URL}/s3/stream/upload-url"
        payload = {"SN": sn, "filename": filename}
        res = requests.post(url, json=payload, timeout=10)
        res.raise_for_status()
        logger.info(f"✅ 영상 URL 발급 성공: {filename}")
        return res.json().get("upload_url")
    except Exception as e:
        logger.error(f"❌ 영상 URL 요청 실패: {filename} - {e}")
        return None


# 파일이 아직 쓰여지고 있는지 확인하는 함수
def is_file_being_written(file_path, wait_time=1):
    """
    파일이 아직 쓰여지고 있는지 확인합니다.
    두 시점의 파일 크기를 비교하여 변화가 있으면 아직 쓰여지고 있다고 판단합니다.
    """
    try:
        # 작은 파일 (1MB 미만)은 이미 완료된 것으로 간주하고 바로 처리
        file_size = os.path.getsize(file_path)
        if file_size < 1024 * 1024:  # 1MB
            return False
            
        time.sleep(wait_time)  # 잠시 대기
        new_size = os.path.getsize(file_path)
        
        return new_size != file_size  # 크기가 다르면 아직 쓰여지고 있음
    except Exception as e:
        logger.error(f"파일 쓰기 상태 확인 오류: {e}")
        return True  # 오류 발생 시 안전하게 True 반환


# 업로드 트래커에서 항목 제거
def remove_from_upload_tracker(file_path):
    """
    업로드 트래커 파일에서 특정 파일 경로 항목을 제거합니다.
    """
    if not os.path.exists(UPLOAD_TRACKER):
        return
        
    try:
        with open(UPLOAD_TRACKER, 'r') as f:
            lines = f.readlines()
            
        with open(UPLOAD_TRACKER, 'w') as f:
            for line in lines:
                if file_path not in line:
                    f.write(line)
    except Exception as e:
        logger.error(f"업로드 트래커 업데이트 오류: {e}")


def upload_and_remove_image(image_path):
    if not os.path.exists(image_path):
        logger.error(f"❌ 이미지 파일 없음: {image_path}")
        return False

    try:
        image_name = os.path.basename(image_path)
        sn = load_sn()
        if not sn:
            logger.error(f"❌ SN 로드 실패, 이미지 삭제: {image_name}")
            if os.path.exists(image_path):
                os.remove(image_path)
            return False

        presigned_url = get_presigned_opencv_url(sn, image_name)
        if not presigned_url:
            logger.error(f"❌ URL 발급 실패, 이미지 삭제: {image_name}")
            if os.path.exists(image_path):
                os.remove(image_path)
            return False

        with open(image_path, "rb") as f:
            logger.info(f"📤 이미지 업로드 시작: {image_name}")
            res = requests.put(
                presigned_url, data=f, headers={"Content-Type": "image/jpeg"}, timeout=30
            )
            if res.status_code == 200:
                logger.info(f"✅ 이미지 업로드 성공: {image_name}")
                if os.path.exists(image_path):
                    os.remove(image_path)
                    logger.info(f"🗑️ 이미지 삭제 완료: {os.path.basename(image_path)}")
                return True
            else:
                logger.error(f"❌ 이미지 업로드 실패: {image_name}, 상태 코드: {res.status_code}")
                return False
    except Exception as e:
        logger.error(f"❌ 이미지 업로드 오류: {image_path} - {e}")
        return False


@with_file_lock
def upload_video_to_s3(video_path):
    if not os.path.exists(video_path):
        logger.error(f"❌ 영상 파일 없음: {video_path}")
        return False

    video_file = os.path.basename(video_path)
    
    # 파일이 완전히 쓰여졌는지 확인
    if is_file_being_written(video_path):
        logger.warning(f"⚠️ 파일이 아직 쓰여지는 중: {video_file}, 건너뜀")
        return False
    
    try:
        logger.info(f"📤 영상 업로드 시작: {video_file}")
        sn = load_sn()
        if not sn:
            logger.error("❌ SN 로드 실패")
            return False

        # 파일 크기 확인 (0 바이트 파일 확인)
        file_size = os.path.getsize(video_path)
        if file_size == 0:
            logger.error(f"❌ 영상 파일이 비어있음: {video_file}")
            os.remove(video_path)
            remove_from_upload_tracker(video_path)
            return False

        presigned_url = get_presigned_video_url(sn, video_file)
        if not presigned_url:
            logger.error(f"❌ 영상 URL 발급 실패: {video_file}")
            return False

        with open(video_path, "rb") as f:
            res = requests.put(
                presigned_url, 
                data=f, 
                headers={"Content-Type": "video/mp4"},
                timeout=60  # 타임아웃 늘림 (60초)
            )
            if res.status_code == 200:
                logger.info(f"✅ 영상 업로드 성공: {video_file}")
                
                # 업로드 트래커에서 해당 항목 제거
                remove_from_upload_tracker(video_path)
                
                # 파일 삭제
                os.remove(video_path)
                logger.info(f"🗑️ 영상 삭제 완료: {video_file}")
                return True
            else:
                logger.error(f"❌ 영상 업로드 실패: {video_file}, 상태 코드: {res.status_code}")
                return False
    except Exception as e:
        logger.error(f"❌ 영상 업로드 오류: {video_path} - {e}")
        return False


def handle_failed_uploads():
    """실패한 업로드를 재시도하는 함수"""
    # 딕셔너리의 복사본으로 반복 (반복 중 딕셔너리 수정을 위해)
    for file_path, retry_count in list(failed_uploads.items()):
        if not os.path.exists(file_path):
            failed_uploads.pop(file_path, None)
            continue
            
        if retry_count >= MAX_RETRY:
            logger.warning(f"⚠️ 최대 재시도 횟수 초과: {file_path}, 건너뜀")
            failed_uploads.pop(file_path, None)
            continue
            
        logger.info(f"🔄 업로드 재시도 ({retry_count+1}/{MAX_RETRY}): {file_path}")
        
        # 파일 유형에 따라 적절한 업로드 함수 호출
        if file_path.endswith('.jpg'):
            success = upload_and_remove_image(file_path)
        elif file_path.endswith('.mp4'):
            success = upload_video_to_s3(file_path)
        else:
            # 알 수 없는 파일 유형
            failed_uploads.pop(file_path, None)
            continue
            
        if success:
            # 성공하면 실패 목록에서 제거
            failed_uploads.pop(file_path, None)
        else:
            # 실패하면 재시도 횟수 증가
            failed_uploads[file_path] = retry_count + 1


def load_upload_tracker():
    """업로드 트래커 파일에서 대기 중인 업로드 목록을 읽어옵니다."""
    if not os.path.exists(UPLOAD_TRACKER):
        # 파일이 없으면 생성
        with open(UPLOAD_TRACKER, 'w') as f:
            pass
        return []
        
    try:
        with open(UPLOAD_TRACKER, 'r') as f:
            lines = f.readlines()
            
        upload_tasks = []
        for line in lines:
            line = line.strip()
            if line:
                parts = line.split('|')
                if len(parts) >= 1:
                    file_path = parts[0]
                    if os.path.exists(file_path):
                        upload_tasks.append(file_path)
        
        return upload_tasks
    except Exception as e:
        logger.error(f"업로드 트래커 로드 오류: {e}")
        return []


def scan_frame_directory():
    try:
        files = [f for f in os.listdir(FRAME_PATH) if f.endswith('.jpg')]
        new_files = [f for f in files if f not in processed_files]

        if new_files:
            logger.info(f"🔍 새 프레임 {len(new_files)}개 발견")

        for filename in new_files:
            # 원본 숫자 형식 파일은 처리하지 않음 (서버에서 이름 변경 예정)
            if filename.split('.')[0].isdigit():
                processed_files.add(filename)
                continue

            # SN_TIMESTAMP 형식의 파일만 처리
            if '_' in filename:
                parts = filename.split('_')
                if len(parts) >= 2 and len(parts[0]) > 5:
                    file_path = os.path.join(FRAME_PATH, filename)
                    logger.info(f"🖼️ 프레임 업로드 큐에 추가: {filename}")
                    
                    # 스레드 풀에 업로드 작업 제출
                    future = image_upload_executor.submit(upload_and_remove_image, file_path)
                    
                    # 콜백 함수를 등록하여 실패 시 failed_uploads에 추가
                    def callback(future, path=file_path):
                        if not future.result() and os.path.exists(path):
                            failed_uploads[path] = failed_uploads.get(path, 0)
                    
                    future.add_done_callback(callback)
                    processed_files.add(filename)
    except Exception as e:
        logger.error(f"❌ 프레임 폴더 스캔 오류: {e}")


def scan_video_directory():
    try:
        # temp_ 로 시작하지 않는 모든 mp4 파일 검색
        files = [f for f in os.listdir(RECORD_PATH) if f.endswith('.mp4') and not f.startswith('temp_')]
        
        # 새로운 영상 파일 식별
        new_files = [f for f in files if f not in processed_videos]

        if new_files:
            logger.info(f"🔍 새 영상 {len(new_files)}개 발견")

        # 현재 시간
        current_time = datetime.now()
        
        # 업로드 트래커에서 대기 중인 작업 확인
        upload_tasks = load_upload_tracker()
        
        # 트래커에는 있지만 처리되지 않은 비디오 확인 및 추가
        for file_path in upload_tasks:
            file_name = os.path.basename(file_path)
            if file_name not in processed_videos and os.path.exists(file_path):
                logger.info(f"📦 업로드 트래커에서 찾은 영상 업로드: {file_name}")
                
                # 스레드 풀을 사용하여 영상 업로드
                future = video_upload_executor.submit(upload_video_to_s3, file_path)
                
                # 콜백 함수를 등록하여 실패 시 failed_uploads에 추가
                def callback(future, path=file_path):
                    if not future.result() and os.path.exists(path):
                        failed_uploads[path] = failed_uploads.get(path, 0)
                
                future.add_done_callback(callback)
                processed_videos.add(file_name)
        
        # 파일명 순으로 정렬해서 시간 순서대로 처리
        new_files.sort()
        
        for filename in new_files:
            file_path = os.path.join(RECORD_PATH, filename)
            
            # 파일이 충분히 "안정화"되었는지 확인 (생성된 후 3초 이상 경과)
            file_creation_time = datetime.fromtimestamp(os.path.getctime(file_path))
            if (current_time - file_creation_time).total_seconds() < 3:
                continue
                
            # 파일이 아직 쓰여지고 있는지 확인
            if is_file_being_written(file_path):
                continue
                
            logger.info(f"📦 영상 업로드 큐에 추가: {filename}")
            
            # 스레드 풀을 사용하여 영상 업로드
            future = video_upload_executor.submit(upload_video_to_s3, file_path)
            
            # 콜백 함수를 등록하여 실패 시 failed_uploads에 추가
            def callback(future, path=file_path):
                if not future.result() and os.path.exists(path):
                    failed_uploads[path] = failed_uploads.get(path, 0)
            
            future.add_done_callback(callback)
            processed_videos.add(filename)
    except Exception as e:
        logger.error(f"❌ 비디오 폴더 스캔 오류: {e}")


def cleanup_stale_entries():
    """processed_files와 processed_videos에서 더 이상 존재하지 않는 파일 항목 제거"""
    global processed_files, processed_videos
    
    # 현재 존재하는 모든 파일 확인
    existing_frames = set(os.listdir(FRAME_PATH))
    existing_videos = set(os.listdir(RECORD_PATH))
    
    # 이미 처리된 파일 목록에서 더 이상 존재하지 않는 항목 제거
    processed_files = {f for f in processed_files if f in existing_frames}
    processed_videos = {f for f in processed_videos if f in existing_videos}
    
    # 주기적으로 로그 출력
    logger.debug(f"현재 추적 중: 프레임 {len(processed_files)}개, 비디오 {len(processed_videos)}개")


if __name__ == "__main__":
    logger.info("🚀 S3 업로드 서비스 시작...")
    logger.info(f"📂 영상 경로: {RECORD_PATH}")
    logger.info(f"📂 프레임 경로: {FRAME_PATH}")

    # 잠금 파일 초기화
    with open(LOCK_FILE, 'w+') as f:
        pass

    try:
        logger.info("🧹 기존 파일 확인 중...")
        scan_frame_directory()
        scan_video_directory()

        logger.info("🔄 주기적 폴더 스캔 시작 (0.5초 간격)")
        
        cleanup_counter = 0  # 정리 작업을 위한 카운터

        while True:
            # 폴더 스캔
            scan_frame_directory()
            scan_video_directory()
            
            # 실패한 업로드 처리
            handle_failed_uploads()
            
            # 300회 스캔마다 (약 2.5분마다) 오래된 항목 정리
            cleanup_counter += 1
            if cleanup_counter >= 300:
                cleanup_stale_entries()
                cleanup_counter = 0
                
            time.sleep(0.5)
    except KeyboardInterrupt:
        logger.info("👋 종료 신호 받음")
        
        # 스레드 풀 정상 종료
        logger.info("🛑 업로드 작업 완료 대기 중...")
        image_upload_executor.shutdown(wait=True)
        video_upload_executor.shutdown(wait=True)
        
        logger.info("✅ S3 업로드 서비스 종료")