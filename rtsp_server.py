#!/usr/bin/env python3
import gi
import sys
import argparse
import signal
import logging
import os
from datetime import datetime, timezone, timedelta

# 로그 비활성화
logging.disable(logging.CRITICAL)

# GStreamer 초기화
gi.require_version('Gst', '1.0')
gi.require_version('GstRtspServer', '1.0')
from gi.repository import Gst, GLib, GstRtspServer

# 시스템 타임존 (KST) 기준 시간 반환
def get_kst_from_running_time(running_time_ns):
    kst = timezone(timedelta(hours=9))
    now = datetime.now(tz=kst)
    elapsed = timedelta(seconds=running_time_ns / 1e9)
    started = now - elapsed
    return started.strftime("record_%Y%m%d_%H%M%S.mp4")

# RTSP 스트리밍 MediaFactory
class TeeRtspMediaFactory(GstRtspServer.RTSPMediaFactory):
    def __init__(self, encoder='mpph265enc', encoder_options="bps=51200000 rc-mode=vbr",
                 payload="rtph265pay", pt=97):
        super().__init__()
        self.encoder = encoder
        self.encoder_options = encoder_options
        self.payload = payload
        self.pt = pt

        self.launch_string = (
            "intervideosrc channel=cam ! "
            "queue ! {0} {1} ! "
            "{2} name=pay0 pt={3}"
        ).format(self.encoder, self.encoder_options, self.payload, self.pt)

    def do_create_element(self, url):
        return Gst.parse_launch(self.launch_string)

# RTSP + 영상 저장 서비스
class RtspRecordingService:
    def __init__(self, device='/dev/video0', port=8554, mount='/test',
                 encoder='mpph265enc', encoder_options="bps=51200000 rc-mode=vbr",
                 payload='rtph265pay', pt=97, record_path="/home/radxa/Videos"):

        self.device = device
        self.port = str(port)
        self.mount = mount
        self.encoder = encoder
        self.encoder_options = encoder_options
        self.payload = payload
        self.pt = pt
        self.record_path = record_path

        Gst.init(None)

        self.server = GstRtspServer.RTSPServer()
        self.server.set_service(self.port)
        self.factory = TeeRtspMediaFactory(encoder, encoder_options, payload, pt)
        self.factory.set_shared(True)
        self.server.get_mount_points().add_factory(self.mount, self.factory)

        self.record_pipeline = self._create_record_pipeline()
        self.loop = GLib.MainLoop()

        self.record_pipeline.get_bus().add_signal_watch()
        self.record_pipeline.get_bus().connect("message::element", self._on_element_message)

    def _create_record_pipeline(self):
        os.makedirs(self.record_path, exist_ok=True)
        file_pattern = os.path.join(self.record_path, "temp_%05d.mp4")

        pipeline_str = (
            f"v4l2src device={self.device} ! "
            "videorate ! "
            "video/x-raw,format=NV12,width=1920,height=1080,framerate=30/1 ! "
            "videoconvert ! "
            "tee name=t "
            "t. ! queue ! "
            f"{self.encoder} {self.encoder_options} ! "
            "h265parse ! splitmuxsink name=smux muxer=mp4mux location={} max-size-time=60000000000 "  # 15분
            "t. ! queue ! intervideosink channel=cam"
        ).format(file_pattern)

        return Gst.parse_launch(pipeline_str)

    def _on_element_message(self, bus, message):
        structure = message.get_structure()
        if not structure:
            return

        if structure.get_name() == "splitmuxsink-fragment-closed":
            location = structure.get_string("location")
            running_time_ns = structure.get_value("running-time")

            if location and os.path.exists(location):
                new_name = get_kst_from_running_time(running_time_ns)
                full_new_path = os.path.join(self.record_path, new_name)
                try:
                    os.rename(location, full_new_path)
                    print(f"📁 저장 완료: {full_new_path}")
                except Exception as e:
                    print(f"❌ 파일 이름 변경 실패: {e}")

    def start(self):
        if self.server.attach(None) == 0:
            sys.exit(1)
        self.record_pipeline.set_state(Gst.State.PLAYING)

    def run(self):
        self.start()
        try:
            self.loop.run()
        except Exception:
            pass
        finally:
            self.stop()

    def stop(self):
        self.record_pipeline.set_state(Gst.State.NULL)
        if self.loop.is_running():
            self.loop.quit()

def signal_handler(sig, frame, service):
    service.stop()
    sys.exit(0)

def parse_args():
    parser = argparse.ArgumentParser(description="24/7 Recording RTSP Server for Radxa")
    parser.add_argument('--device', default='/dev/video0')
    parser.add_argument('--port', type=int, default=8554)
    parser.add_argument('--mount', default='/test')
    parser.add_argument('--encoder', default='mpph265enc')
    parser.add_argument('--encoder-options', default='bps=51200000 rc-mode=vbr')
    parser.add_argument('--payload', default='rtph265pay')
    parser.add_argument('--pt', type=int, default=97)
    parser.add_argument('--record-path', default='/home/radxa/Videos')
    return parser.parse_args()

def main():
    args = parse_args()
    service = RtspRecordingService(
        device=args.device,
        port=args.port,
        mount=args.mount,
        encoder=args.encoder,
        encoder_options=args.encoder_options,
        payload=args.payload,
        pt=args.pt,
        record_path=args.record_path
    )
    signal.signal(signal.SIGINT, lambda s, f: signal_handler(s, f, service))
    signal.signal(signal.SIGTERM, lambda s, f: signal_handler(s, f, service))
    service.run()

if __name__ == '__main__':
    main()
