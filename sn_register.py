# sn_register.py

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
    except Exception as e:
        print(f"❌ IP 조회 실패: {e}")
        return None

def register_device(ip):
    try:
        url = f"{API_HOST}/device/register"
        response = requests.get(url, params={"ip": ip}, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"❌ 등록 요청 실패: {e}")
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
    except requests.exceptions.RequestException as e:
        print(f"❌ 업데이트 요청 실패: {e}")
        return None

def save_sn(sn):
    try:
        with open(SN_FILE, "w") as f:
            f.write(sn)
        return True
    except Exception as e:
        print(f"❌ SN 저장 실패: {e}")
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
        print(f"📦 SN 파일 확인됨: {sn} → 업데이트 요청")
        result = update_device(sn, ip)
        if result:
            print("✅ 업데이트 성공:", json.dumps(result, indent=2, ensure_ascii=False))
    else:
        print("🆕 SN 없음 → 신규 등록 요청")
        result = register_device(ip)
        if result:
            print("✅ 등록 성공:", json.dumps(result, indent=2, ensure_ascii=False))
            new_sn = result.get("serial_number")
            if new_sn:
                if save_sn(new_sn):
                    print(f"📄 SN 저장 완료: {new_sn}")
                else:
                    print("❌ SN 저장 실패")
            else:
                print("❌ 응답에 serial_number 없음")

if __name__ == "__main__":
    main()
