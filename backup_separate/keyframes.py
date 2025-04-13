# opencv
import cv2
import os
from datetime import datetime, timedelta

VIDEO_DIR = "/home/radxa/Videos"
OUTPUT_DIR = "/home/radxa/keyframes"
MOTION_THRESHOLD = 5000

os.makedirs(OUTPUT_DIR, exist_ok=True)
video_files = sorted([f for f in os.listdir(VIDEO_DIR) if f.endswith(".mp4")])

for video_file in video_files:
    print(f"🎞️ 처리 중: {video_file}")
    filepath = os.path.join(VIDEO_DIR, video_file)

    try:
        # record_20250410_155320 → 추출
        base_name = os.path.splitext(video_file)[0]
        time_str = base_name.replace("record_", "")
        creation_time = datetime.strptime(time_str, "%Y%m%d_%H%M%S")

        cap = cv2.VideoCapture(filepath)
        if not cap.isOpened():
            print(f"⚠️ 열 수 없음: {video_file}")
            continue

        fps = int(cap.get(cv2.CAP_PROP_FPS))
        print(f"FPS: {fps}")

        ret, prev_frame = cap.read()
        if not ret:
            print(f"❌ 첫 프레임 읽기 실패: {video_file}")
            cap.release()
            continue

        frame_id = 0
        while True:
            ret, frame = cap.read()
            if not ret or frame is None:
                break

            if frame_id % fps == 0:
                gray_prev = cv2.cvtColor(prev_frame, cv2.COLOR_BGR2GRAY)
                gray_current = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)

                diff = cv2.absdiff(gray_prev, gray_current)
                _, diff_thresh = cv2.threshold(diff, 30, 255, cv2.THRESH_BINARY)
                motion_score = cv2.countNonZero(diff_thresh)

                if motion_score > MOTION_THRESHOLD:
                    seconds = frame_id // fps
                    timestamp = creation_time + timedelta(seconds=seconds)
                    timestamp_str = timestamp.strftime("%Y-%m-%d-%H-%M-%S")
                    filename = f"record_{timestamp_str}.jpg"
                    save_path = os.path.join(OUTPUT_DIR, filename)
                    cv2.imwrite(save_path, frame)
                    print(f" 저장됨: {filename}")

                prev_frame = frame

            frame_id += 1

        cap.release()

    except Exception as e:
        print(f"❌ 처리 실패: {video_file} - {e}")

print("✅ 전체 처리 완료")
