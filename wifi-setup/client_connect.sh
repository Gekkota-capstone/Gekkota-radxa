#!/bin/bash

SSID=$1
PASSWORD=$2

wpa_passphrase "$SSID" "$PASSWORD" > /etc/wpa_supplicant/wpa_supplicant.conf

systemctl stop hostapd
systemctl stop dnsmasq

ip link set wlan0 down
ip addr flush dev wlan0
ip link set wlan0 up

wpa_cli -i wlan0 reconfigure
sleep 10
