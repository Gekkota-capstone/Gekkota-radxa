# sn_register and rtsp_server

# rstp_server.py

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


def get_previous_minute_timestamp():
    kst = timezone(timedelta(hours=9))
    started = datetime.now(tz=kst) - timedelta(minutes=1)
    started = started.replace(second=0, microsecond=0)
    return started.strftime("%Y%m%d_%H%M%S")


def rename_multifilesink_frames(sn, video_timestamp, frame_dir="/home/radxa/Frames"):
    base_time = datetime.strptime(video_timestamp, "%Y%m%d_%H%M%S")
    try:
        frame_files = sorted(
            [
                f
                for f in os.listdir(frame_dir)
                if f.startswith("frame_") and f.endswith(".jpg")
            ],
            key=lambda x: int(x.split("_")[1].split(".")[0]),
        )[-60:]
    except:
        frame_files = sorted(
            [
                f
                for f in os.listdir(frame_dir)
                if f.startswith("frame_") and f.endswith(".jpg")
            ],
            key=lambda x: os.path.getmtime(os.path.join(frame_dir, x)),
        )[-60:]

    for i, filename in enumerate(frame_files):
        old_path = os.path.join(frame_dir, filename)
        timestamp = base_time + timedelta(seconds=i)
        new_name = f"{sn}_{timestamp.strftime('%Y%m%d_%H%M%S')}.jpg"
        new_path = os.path.join(frame_dir, new_name)
        try:
            if os.path.exists(new_path):
                os.remove(new_path)
            shutil.move(old_path, new_path)
        except:
            pass


def wait_until_next_minute():
    now = datetime.utcnow()
    next_min = (now + timedelta(minutes=1)).replace(second=0, microsecond=0)
    wait_sec = (next_min - now).total_seconds()
    time.sleep(wait_sec)


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

        wait_until_next_minute()
        self.record_pipeline = self._create_record_pipeline()

        self.loop = GLib.MainLoop()
        self.record_pipeline.get_bus().add_signal_watch()
        self.record_pipeline.get_bus().connect(
            "message::element", self._on_element_message
        )

    def _create_record_pipeline(self):
        video_pattern = os.path.join(self.record_path, "temp_%05d.mp4")
        frame_pattern = os.path.join(self.frame_path, "frame_%05d.jpg")
        pipeline_str = (
            f"v4l2src device={self.device} ! "
            "videorate ! video/x-raw,format=NV12,width=1920,height=1080,framerate=30/1 ! tee name=t "
            "t. ! queue leaky=downstream max-size-buffers=5 ! "
            f"{self.encoder} {self.encoder_options} ! h265parse ! "
            "splitmuxsink name=smux muxer=mp4mux async-finalize=true location={} max-size-time=60000000000 "
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
                timestamp = get_previous_minute_timestamp()
                new_video_path = os.path.join(
                    self.record_path, f"{DEVICE_SN}_{timestamp}.mp4"
                )
                try:
                    if os.path.exists(new_video_path):
                        os.remove(new_video_path)
                    os.rename(location, new_video_path)
                    rename_multifilesink_frames(DEVICE_SN, timestamp, self.frame_path)
                except:
                    pass

    def start(self):
        if self.server.attach(None) == 0:
            sys.exit(1)
        self.record_pipeline.set_state(Gst.State.PLAYING)

    def run(self):
        self.start()
        try:
            self.loop.run()
        except:
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
        sys.exit(1)

    sn = load_sn()
    if sn and sn != "UNKNOWN":
        update_device(sn, ip)
        DEVICE_SN = sn
    else:
        result = register_device(ip)
        if result:
            new_sn = result.get("serial_number")
            if new_sn:
                save_sn(new_sn)
                DEVICE_SN = new_sn
            else:
                sys.exit(1)
        else:
            sys.exit(1)

    if DEVICE_SN == "UNKNOWN":
        sys.exit(1)

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
