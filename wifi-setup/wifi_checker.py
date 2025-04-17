import os
import subprocess

def is_wifi_configured():
    path = "/etc/wpa_supplicant/wpa_supplicant.conf"
    return os.path.exists(path) and "psk=" in open(path).read()

if not is_wifi_configured():
    print("Wi-Fi 설정 없음 → AP 모드로 진입")
    subprocess.run(["bash", "/home/radxa/wifi-setup/ap_setup.sh"])
    subprocess.run(["python3", "/home/radxa/wifi-setup/web/main.py"])
