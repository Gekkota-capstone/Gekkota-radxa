#!/bin/bash

# 고정 IP 할당
ip link set wlan0 down
ip addr flush dev wlan0
ip link set wlan0 up
ip addr add 192.168.4.1/24 dev wlan0

# hostapd 실행
systemctl start hostapd
systemctl start dnsmasq
