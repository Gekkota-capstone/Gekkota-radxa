#!/bin/bash

# Start RTSP server
/usr/bin/python3 /home/radxa/rtsp_server.py &
RTSP_PID=$!

# Wait to ensure RTSP server is running
sleep 5

# Start S3 upload service
/usr/bin/python3 /home/radxa/s3_upload.py &