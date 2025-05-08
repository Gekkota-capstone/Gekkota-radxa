import gi
import sys
import argparse
import signal
import logging
import os
import shutil
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
import socket
import requests
import json

gi.require_version("Gst", "1.0")
gi.require_version("GstRtspServer", "1.0")
from gi.repository import Gst, GLib, GstRtspServer

logging.disable(logging.CRITICAL)

API_HOST = "https://api.saffir.co.kr"
SN_FILE = "/home/radxa/sn.txt"
RTSP_PORT = 8554
RTSP_PATH = "stream"


def get_local_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except:
        return None


def register_device(ip):
    try:
        url = f"{API_HOST}/device/register"
        response = requests.get(url, params={"ip": ip}, timeout=10)
        response.raise_for_status()
        return response.json()
    except:
        return None


def update_device(sn, ip):
    try:
        url = f"{API_HOST}/device/{sn}"
        payload = {
            "SN": sn,
            "IP": ip,
            "rtsp_url": f"rtsp://{ip}:{RTSP_PORT}/{RTSP_PATH}",
        }
        headers = {"Content-Type": "application/json"}
        response = requests.put(
            url, data=json.dumps(payload), headers=headers, timeout=10
        )
        response.raise_for_status()
        return response.json()
    except:
        return None


def save_sn(sn):
    try:
        with open(SN_FILE, "w") as f:
            f.write(sn)
        return True
    except:
        return False


def load_sn():
    try:
        sn_path = Path(SN_FILE)
        if sn_path.exists():
            return sn_path.read_text().strip()
    except:
        pass
    return "UNKNOWN"


DEVICE_SN = load_sn()


# 5분 전 타임스탬프 생성 (00초로 설정)
def get_previous_five_minutes_timestamp():
    kst = timezone(timedelta(hours=9))
    now = datetime.now(tz=kst)
    # 현재 시간의 분을 5로 나눈 나머지를 계산
    remainder = now.minute % 5
    # 가장 최근 5분 간격 시간을 계산 (현재 시간에서 나머지 분만큼 뺌)
    prev_five_min = now - timedelta(minutes=remainder, seconds=now.second, microseconds=now.microsecond)
    
    # 5분 전 시간 계산
    prev_five_min = prev_five_min - timedelta(minutes=5)
    
    return prev_five_min.strftime("%Y%m%d_%H%M%S")


# 다음 5분 간격까지 대기
def wait_until_next_five_minutes():
    now = datetime.utcnow()
    # 현재 분을 5로 나눈 나머지 계산
    remainder_minutes = now.minute % 5
    remainder_seconds = now.second
    remainder_microseconds = now.microsecond
    
    # 다음 5분 간격 계산
    wait_minutes = 5 - remainder_minutes
    if remainder_minutes == 0 and (remainder_seconds > 0 or remainder_microseconds > 0):
        wait_minutes = 5
    
    # 다음 5분 간격의 정확한 시간
    next_five_min = now + timedelta(
        minutes=wait_minutes, 
        seconds=-remainder_seconds, 
        microseconds=-remainder_microseconds
    )
    
    wait_sec = (next_five_min - now).total_seconds()
    print(f"🕒 다음 5분 간격까지 {wait_sec:.2f}초 대기 중...")
    time.sleep(wait_sec)
    print("⏰ 대기 완료, 서버 시작")


# 현재 시간 기준 타임스탬프 생성 (실시간 프레임용)
def get_current_timestamp():
    kst = timezone(timedelta(hours=9))
    now = datetime.now(tz=kst)
    return now.strftime("%Y%m%d_%H%M%S")


# GStreamer 버스 메시지 콜백 함수 (현재 시간 기준 파일명 생성)
def frame_file_created_callback(bus, message, user_data):
    if message.type == Gst.MessageType.ELEMENT:
        structure = message.get_structure()
        if structure and structure.get_name() == "GstMultiFileSink":
            filename = structure.get_string("filename")
            if filename:
                # 파일 존재 확인
                if not os.path.exists(filename):
                    return
                    
                # 기존 frame_XXXXX.jpg 파일을 SN_TIMESTAMP.jpg 형식으로 직접 변경
                timestamp = get_current_timestamp()
                new_filename = os.path.join(os.path.dirname(filename), f"{DEVICE_SN}_{timestamp}.jpg")
                
                try:
                    # 동일 이름의 파일이 있으면 삭제
                    if os.path.exists(new_filename):
                        os.remove(new_filename)
                    # 파일 이동
                    shutil.move(filename, new_filename)
                    print(f"✅ 프레임 생성: {os.path.basename(new_filename)}")
                except Exception as e:
                    print(f"❌ 프레임 이름 변경 실패: {filename} -> {new_filename} - {e}")


class TeeRtspMediaFactory(GstRtspServer.RTSPMediaFactory):
    def __init__(
        self,
        encoder="mpph265enc",
        encoder_options="bps=51200000 rc-mode=vbr",
        payload="rtph265pay",
        pt=97,
    ):
        super().__init__()
        self.encoder = encoder
        self.encoder_options = encoder_options
        self.payload = payload
        self.pt = pt
        self.launch_string = (
            "intervideosrc channel=cam ! queue leaky=downstream max-size-buffers=5 ! "
            "{0} {1} ! {2} name=pay0 pt={3}"
        ).format(self.encoder, self.encoder_options, self.payload, self.pt)

    def do_create_element(self, url):
        return Gst.parse_launch(self.launch_string)


class RtspRecordingService:
    def __init__(
        self,
        device="/dev/video0",
        port=8554,
        mount="/stream",
        encoder="mpph265enc",
        encoder_options="bps=51200000 rc-mode=vbr",
        payload="rtph265pay",
        pt=97,
        record_path="/home/radxa/Videos",
        frame_path="/home/radxa/Frames",
    ):

        self.device = device
        self.port = str(port)
        self.mount = mount
        self.encoder = encoder
        self.encoder_options = encoder_options
        self.payload = payload
        self.pt = pt
        self.record_path = record_path
        self.frame_path = frame_path

        Gst.init(None)
        self.server = GstRtspServer.RTSPServer()
        self.server.set_service(self.port)
        self.server.props.backlog = 2

        self.factory = TeeRtspMediaFactory(encoder, encoder_options, payload, pt)
        self.factory.set_shared(True)
        self.server.get_mount_points().add_factory(self.mount, self.factory)

        os.makedirs(self.record_path, exist_ok=True)
        os.makedirs(self.frame_path, exist_ok=True)

        # 시작 전 기존 프레임 정리
        self._cleanup_existing_frames()

        # 다음 5분 간격까지 대기
        wait_until_next_five_minutes()
        
        # 녹화 파이프라인 생성
        self.record_pipeline = self._create_record_pipeline()

        # 프레임 생성 콜백 연결
        self.record_pipeline.get_bus().add_signal_watch()
        self.record_pipeline.get_bus().connect("message", frame_file_created_callback, None)
        self.record_pipeline.get_bus().connect("message::element", self._on_element_message)

        self.loop = GLib.MainLoop()

    def _cleanup_existing_frames(self):
        try:
            count = 0
            for filename in os.listdir(self.frame_path):
                if filename.endswith(".jpg"):
                    file_path = os.path.join(self.frame_path, filename)
                    os.remove(file_path)
                    count += 1
            if count > 0:
                print(f"🧹 기존 프레임 {count}개 정리 완료")
        except Exception as e:
            print(f"❌ 기존 프레임 정리 실패: {e}")

    def _create_record_pipeline(self):
        video_pattern = os.path.join(self.record_path, "temp_%05d.mp4")
        
        # 프레임 패턴을 timestamp로 직접 저장하지 않고, 
        # GStreamer에서는 고유한 이름으로 만들고 메시지 핸들러에서 이름 변경
        frame_pattern = os.path.join(self.frame_path, "%d.jpg")
        
        pipeline_str = (
            f"v4l2src device={self.device} ! "
            "videorate ! video/x-raw,format=NV12,width=1920,height=1080,framerate=30/1 ! tee name=t "
            "t. ! queue leaky=downstream max-size-buffers=5 ! "
            f"{self.encoder} {self.encoder_options} ! h265parse ! "
            "splitmuxsink name=smux muxer=mp4mux async-finalize=true location={} max-size-time=300000000000 "  # 5분 = 300초 = 300,000,000,000 나노초
            "t. ! queue leaky=downstream max-size-buffers=5 ! "
            "videorate ! video/x-raw,framerate=1/1 ! jpegenc ! multifilesink location={} post-messages=true "
            "t. ! queue leaky=downstream max-size-buffers=5 ! intervideosink channel=cam"
        ).format(video_pattern, frame_pattern)
        
        return Gst.parse_launch(pipeline_str)

    def _on_element_message(self, bus, message):
        structure = message.get_structure()
        if not structure:
            return
        if structure.get_name() == "splitmuxsink-fragment-closed":
            location = structure.get_string("location")
            if location and os.path.exists(location):
                timestamp = get_previous_five_minutes_timestamp()
                new_video_path = os.path.join(
                    self.record_path, f"{DEVICE_SN}_{timestamp}.mp4"
                )
                try:
                    if os.path.exists(new_video_path):
                        os.remove(new_video_path)
                    os.rename(location, new_video_path)
                    print(f"✅ 비디오 리네이밍: {location} → {new_video_path}")
                except Exception as e:
                    print(f"❌ 파일 변경 실패: {e}")

    def start(self):
        if self.server.attach(None) == 0:
            print("❌ RTSP 서버 연결 실패")
            sys.exit(1)
        print("✅ RTSP 서버 연결 성공")
        self.record_pipeline.set_state(Gst.State.PLAYING)
        print("✅ 녹화 파이프라인 시작")

    def run(self):
        self.start()
        try:
            self.loop.run()
        except Exception as e:
            print(f"❌ 메인 루프 오류: {e}")
        finally:
            self.stop()

    def stop(self):
        self.record_pipeline.set_state(Gst.State.NULL)
        if self.loop.is_running():
            self.loop.quit()
        print("✅ 서비스 정상 종료")


def signal_handler(sig, frame, service):
    print("👋 종료 신호 받음")
    service.stop()
    sys.exit(0)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--device", default="/dev/video0")
    parser.add_argument("--port", type=int, default=8554)
    parser.add_argument("--mount", default="/stream")
    parser.add_argument("--encoder", default="mpph265enc")
    parser.add_argument("--encoder-options", default="bps=51200000 rc-mode=vbr")
    parser.add_argument("--payload", default="rtph265pay")
    parser.add_argument("--pt", type=int, default=97)
    parser.add_argument("--record-path", default="/home/radxa/Videos")
    parser.add_argument("--frame-path", default="/home/radxa/Frames")
    return parser.parse_args()


def main():
    global DEVICE_SN

    ip = get_local_ip()
    if not ip:
        print("❌ 로컬 IP 주소 확인 실패")
        sys.exit(1)
    print(f"✅ 로컬 IP 주소: {ip}")

    sn = load_sn()
    if sn and sn != "UNKNOWN":
        print(f"✅ 기존 SN 확인: {sn}")
        update_device(sn, ip)
        DEVICE_SN = sn
    else:
        print("ℹ️ SN 없음, 새로 등록 시도")
        result = register_device(ip)
        if result:
            new_sn = result.get("serial_number")
            if new_sn:
                save_sn(new_sn)
                DEVICE_SN = new_sn
                print(f"✅ 새 SN 등록 완료: {new_sn}")
            else:
                print("❌ SN 응답 없음")
                sys.exit(1)
        else:
            print("❌ 디바이스 등록 실패")
            sys.exit(1)

    if DEVICE_SN == "UNKNOWN":
        print("❌ 유효한 SN 없음")
        sys.exit(1)

    print(f"🚀 RTSP 서버 시작 (SN: {DEVICE_SN})")
    args = parse_args()
    service = RtspRecordingService(
        device=args.device,
        port=args.port,
        mount=args.mount,
        encoder=args.encoder,
        encoder_options=args.encoder_options,
        payload=args.payload,
        pt=args.pt,
        record_path=args.record_path,
        frame_path=args.frame_path,
    )
    signal.signal(signal.SIGINT, lambda s, f: signal_handler(s, f, service))
    signal.signal(signal.SIGTERM, lambda s, f: signal_handler(s, f, service))
    service.run()


if __name__ == "__main__":
    main()