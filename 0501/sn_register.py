import socket
import requests
import json
import os

API_HOST = "https://api.saffir.co.kr"
SN_FILE = "sn.txt"
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
            "rtsp_url": f"rtsp://{ip}:{RTSP_PORT}/{RTSP_PATH}"
        }
        headers = {"Content-Type": "application/json"}
        response = requests.put(url, data=json.dumps(payload), headers=headers, timeout=10)
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
    if os.path.exists(SN_FILE):
        with open(SN_FILE, "r") as f:
            return f.read().strip()
    return None

def main():
    ip = get_local_ip()
    if not ip:
        return
    sn = load_sn()
    if sn:
        update_device(sn, ip)
    else:
        result = register_device(ip)
        if result:
            new_sn = result.get("serial_number")
            if new_sn:
                save_sn(new_sn)

if __name__ == "__main__":
    main()
