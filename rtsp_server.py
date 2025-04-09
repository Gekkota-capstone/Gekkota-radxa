#!/usr/bin/env python3
import gi
import sys
import argparse
import signal
import socket
import logging
import os

# 로그 비활성화
logging.disable(logging.CRITICAL)

gi.require_version('Gst', '1.0')
gi.require_version('GstRtspServer', '1.0')
from gi.repository import Gst, GLib, GstRtspServer

class TeeRtspMediaFactory(GstRtspServer.RTSPMediaFactory):
    def __init__(self, encoder='mpph265enc', encoder_options="bps=51200000 rc-mode=vbr",
                 payload="rtph265pay", pt=97):
        super(TeeRtspMediaFactory, self).__init__()
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

class RtspRecordingService:
    def __init__(self, device='/dev/video0', port=8554, mount='/test',
                 encoder='mpph265enc', encoder_options="bps=51200000 rc-mode=vbr",
                 payload="rtph265pay", pt=97, record_path="/home/radxa/Videos"):

        self.device = device
        self.port = str(port)
        self.mount = mount
        self.encoder = encoder
        self.encoder_options = encoder_options
        self.payload = payload
        self.pt = pt
        self.record_path = record_path

        Gst.init(None)

        # RTSP 서버
        self.server = GstRtspServer.RTSPServer()
        self.server.set_service(self.port)
        self.factory = TeeRtspMediaFactory(
            encoder, encoder_options, payload, pt
        )
        self.factory.set_shared(True)
        self.server.get_mount_points().add_factory(self.mount, self.factory)

        # 녹화용 파이프라인
        self.record_pipeline = self._create_record_pipeline()

        self.loop = GLib.MainLoop()

    def _create_record_pipeline(self):
        os.makedirs(self.record_path, exist_ok=True)

        file_pattern = os.path.join(self.record_path, "record_%05d.mp4")

        pipeline_str = (
            f"v4l2src device={self.device} ! "
            "videoconvert ! video/x-raw,format=NV12,width=1920,height=1080,framerate=60/1 ! "
            "tee name=t t. ! queue ! "
            f"{self.encoder} {self.encoder_options} ! "
            "h265parse ! splitmuxsink muxer=mp4mux location={} max-size-time=60000000000 "
            "t. ! queue ! intervideosink channel=cam"
        ).format(file_pattern)

        return Gst.parse_launch(pipeline_str)

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
