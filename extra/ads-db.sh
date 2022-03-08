#!/bin/bash
# dump1090exporter --port=9105   --latitude=32.78   --longitude=-79.9419   --debug --url=http://127.0.0.1/dump1090 >> /tmp/dump-exporter.log 2>> /tmp/dump-exporter.log&
cd /home/pi/ads-db
source venv/bin/activate
./ads-db.py -D -sc 50 -rs 127.0.0.1
