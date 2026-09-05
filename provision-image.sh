#!/usr/bin/env bash
#
# provision-image.sh
#
# Turns a stock Raspberry Pi OS Bookworm (Lite, 64-bit) install into the
# brine tank sensor appliance: wifi-fallback captive portal (comitup),
# an auto-generated per-device hostname (no naming step for the customer --
# see step 7b below), the A02YYUW distance sensor driver, and a systemd
# timer that publishes readings to AWS IoT Core -- everything this device
# needs except its own AWS certificate, which gets filled in later by
# onboard-device.sh (see the AWS IoT onboarding runbook).
#
# HOW TO RUN THIS
#   Phase A (build the golden image, once):
#     1. Flash Raspberry Pi OS Bookworm Lite (64-bit) with Raspberry Pi
#        Imager. In Customisation: set a hostname (anything -- this
#        reference Pi's own name is thrown away before capture), enable SSH,
#        set a username/password -- and, since the Zero 2 W has no Ethernet
#        port, also set the wireless LAN to your real home network for this
#        one build run only (sanitize-for-imaging.sh wipes it before capture).
#     2. Boot it and SSH in over that wifi -- no cable needed.
#     3. Copy this script over, then:
#          sudo TARGET_USER=<your-username> ./provision-image.sh
#     4. Verify (see the runbook's checklist), then run
#        sanitize-for-imaging.sh and capture the SD card as golden.img.
#   Phase B (every new device after that):
#     Flash golden.img as-is (no Customisation available or needed for a
#     custom image) and power it on -- each device names itself from its own
#     CPU serial on first boot. Customer-facing steps are just wifi setup;
#     this script never runs again.
#
# Safe to re-run (each step checks before it changes anything).

set -euo pipefail

if [ "$(id -u)" -ne 0 ]; then
  echo "Run this as root: sudo ./provision-image.sh" >&2
  exit 1
fi

TARGET_USER="${TARGET_USER:-pi}"
TARGET_HOME="/home/$TARGET_USER"
SENSOR_DIR="/opt/brine-sensor"
CERT_DIR="$TARGET_HOME/certs"
LOGFILE="/var/log/provision-image.log"

log() { echo "[provision $(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOGFILE"; }

log "=== provision-image.sh starting (TARGET_USER=$TARGET_USER) ==="

# ---------------------------------------------------------------------------
# 0. Wait for network -- needed whether this runs over SSH or from firstrun.sh
# ---------------------------------------------------------------------------
log "Waiting for network..."
for i in $(seq 1 60); do
  if getent hosts deb.debian.org >/dev/null 2>&1; then
    log "Network is up."
    break
  fi
  if [ "$i" -eq 60 ]; then
    log "ERROR: no network after 5 minutes. Connect Ethernet and re-run."
    exit 1
  fi
  sleep 5
done

# ---------------------------------------------------------------------------
# 0b. Stop apt's own background update timers for this run -- on a fresh
#     image they can fire (and re-fire) as soon as the network comes up,
#     repeatedly grabbing the dpkg lock this script also needs. This only
#     stops them for the current boot; it doesn't disable them going
#     forward, and sanitize-for-imaging.sh / the next flash resets the
#     device anyway.
# ---------------------------------------------------------------------------
log "Stopping apt-daily timers for this run so they don't fight for the package lock..."
systemctl stop apt-daily.service apt-daily-upgrade.service 2>/dev/null || true
systemctl stop apt-daily.timer apt-daily-upgrade.timer 2>/dev/null || true

# ---------------------------------------------------------------------------
# 1. Base packages
# ---------------------------------------------------------------------------
# Fresh images run apt-daily/apt-daily-upgrade in the background as soon as
# they get network access, which can be mid-download when we get here and
# holds the same dpkg lock apt-get needs. Rather than fail, wait it out.
apt_get_retry() {
  local out i
  for i in $(seq 1 60); do
    if out="$(apt-get "$@" 2>&1)"; then
      echo "$out"
      return 0
    fi
    if echo "$out" | grep -qi 'could not get lock\|dpkg frontend lock\|resource temporarily unavailable\|frontend is locked'; then
      [ "$i" -eq 1 ] && log "Package manager is locked (likely apt-daily running after first boot) -- waiting for it to finish..."
      sleep 5
      continue
    fi
    echo "$out" >&2
    log "ERROR: apt-get $* failed for a reason other than a lock -- see above."
    exit 1
  done
  log "ERROR: dpkg/apt lock still held after 5 minutes. Check 'ps aux | grep apt' on the Pi and re-run this script."
  exit 1
}
dpkg_retry() {
  local out i
  for i in $(seq 1 60); do
    if out="$(dpkg "$@" 2>&1)"; then
      echo "$out"
      return 0
    fi
    if echo "$out" | grep -qi 'could not get lock\|dpkg frontend lock\|resource temporarily unavailable\|frontend is locked'; then
      [ "$i" -eq 1 ] && log "Package manager is locked (likely apt-daily running after first boot) -- waiting for it to finish..."
      sleep 5
      continue
    fi
    echo "$out" >&2
    return 1
  done
  log "ERROR: dpkg/apt lock still held after 5 minutes. Check 'ps aux | grep apt' on the Pi and re-run this script."
  exit 1
}

log "Installing base packages..."
export DEBIAN_FRONTEND=noninteractive
apt_get_retry update
apt_get_retry install -y --no-install-recommends \
  python3-pip python3-serial git curl ca-certificates jq

# ---------------------------------------------------------------------------
# 2. UART setup for the A02YYUW (wired to GPIO14/15, read as /dev/ttyAMA0)
#    - disable-bt frees the full PL011 UART (ttyAMA0) from Bluetooth onto
#      the GPIO pins, since the DFRobot driver opens /dev/ttyAMA0 directly
#    - the login console must be off that UART, or it steals the sensor's
#      bytes
# ---------------------------------------------------------------------------
log "Configuring UART for the distance sensor..."
BOOT_CFG=/boot/firmware/config.txt
BOOT_CMDLINE=/boot/firmware/cmdline.txt

grep -q '^dtoverlay=disable-bt' "$BOOT_CFG" || echo 'dtoverlay=disable-bt' >> "$BOOT_CFG"
grep -q '^enable_uart=1' "$BOOT_CFG" || echo 'enable_uart=1' >> "$BOOT_CFG"

if grep -qE 'console=(serial0|ttyAMA0),[0-9]+' "$BOOT_CMDLINE"; then
  sed -i -E 's/console=(serial0|ttyAMA0),[0-9]+ //' "$BOOT_CMDLINE"
  log "Removed serial console from cmdline.txt."
fi

systemctl disable --now hciuart.service 2>/dev/null || true
systemctl disable --now serial-getty@ttyAMA0.service 2>/dev/null || true
systemctl mask serial-getty@ttyAMA0.service 2>/dev/null || true

# ---------------------------------------------------------------------------
# 3. comitup: broadcasts an AP + captive portal whenever no known wifi
#    network is reachable, remembers networks once configured, requires
#    NetworkManager (default on Bookworm). No config needed for default
#    behavior (AP named comitup-<nnn>).
# ---------------------------------------------------------------------------
if ! dpkg -s comitup >/dev/null 2>&1; then
  log "Installing comitup (wifi fallback AP + captive portal)..."
  TMP="$(mktemp -d)"
  ARCHIVE_URL="https://davesteele.github.io/comitup/archive.html"
  DEB_REL_PATH="$(curl -fsSL "$ARCHIVE_URL" \
    | grep -oE 'deb/davesteele-comitup-apt-source_[0-9.]+_all\.deb' \
    | sort -V | tail -1)"
  if [ -z "$DEB_REL_PATH" ]; then
    log "ERROR: couldn't find the comitup apt-source package on $ARCHIVE_URL"
    log "       Install manually -- see https://github.com/davesteele/comitup/wiki/Installing-Comitup"
    exit 1
  fi
  curl -fsSL "https://davesteele.github.io/comitup/$DEB_REL_PATH" -o "$TMP/comitup-apt-source.deb"
  dpkg_retry -i "$TMP/comitup-apt-source.deb" || apt_get_retry install -f -y
  apt_get_retry update
  apt_get_retry install -y comitup
  rm -rf "$TMP"
else
  log "comitup already installed, skipping."
fi
systemctl enable --now comitup 2>/dev/null || true

log "Registering the comitup state-change callback..."
COMITUP_CONF=/etc/comitup.conf
touch "$COMITUP_CONF"
if grep -q '^external_callback' "$COMITUP_CONF"; then
  sed -i "s#^external_callback.*#external_callback = $SENSOR_DIR/comitup-callback.sh#" "$COMITUP_CONF"
else
  echo "external_callback = $SENSOR_DIR/comitup-callback.sh" >> "$COMITUP_CONF"
fi

# ---------------------------------------------------------------------------
# 4. Python deps for the sensor + IoT publish script
# ---------------------------------------------------------------------------
log "Installing Python packages (pyserial, awsiotsdk)..."
pip3 install --break-system-packages --quiet pyserial awsiotsdk

# ---------------------------------------------------------------------------
# 5. Deploy the sensor driver + publish script, embedded verbatim below so
#    this file has no other runtime dependency (nothing to git clone).
# ---------------------------------------------------------------------------
log "Deploying sensor driver + publish script to $SENSOR_DIR..."
mkdir -p "$SENSOR_DIR/DFRobot_RaspberryPi_A02YYUW"

cat > "$SENSOR_DIR/DFRobot_RaspberryPi_A02YYUW/DFRobot_RaspberryPi_A02YYUW.py" <<'DRIVER_EOF'
# -*- coding:utf-8 -*-

'''!
  @file DFRobot_RaspberryPi_A02YYUW.py
  @brief Ranging distance sensor。
  @copyright   Copyright (c) 2010 DFRobot Co.Ltd (http://www.dfrobot.com)
  @license     The MIT License (MIT)
  @author      Arya(xue.peng@dfrobot.com)
  @version     V1.0
  @date        2021-08-30
  @url https://github.com/DFRobot/DFRobot_RaspberryPi_A02YYUW
'''

import serial

import time

class DFRobot_A02_Distance:

  ## Board status
  STA_OK = 0x00
  STA_ERR_CHECKSUM = 0x01
  STA_ERR_SERIAL = 0x02
  STA_ERR_CHECK_OUT_LIMIT = 0x03
  STA_ERR_CHECK_LOW_LIMIT = 0x04
  STA_ERR_DATA = 0x05

  ## last operate status, users can use this variable to determine the result of a function call.
  last_operate_status = STA_OK

  ## variable
  distance = 0

  ## Maximum range
  distance_max = 4500
  distance_min = 0
  range_max = 4500

  def __init__(self):
    '''
      @brief    Sensor initialization.
    '''
    self._ser = serial.Serial("/dev/ttyAMA0", 9600)
    if self._ser.isOpen() != True:
      self.last_operate_status = self.STA_ERR_SERIAL

  def set_dis_range(self, min, max):
    self.distance_max = max
    self.distance_min = min

  def getDistance(self):
    '''
      @brief    Get measured distance
      @return    measured distance
    '''
    self._measure()
    return self.distance

  def _check_sum(self, l):
    return (l[0] + l[1] + l[2])&0x00ff

  def _measure(self):
    data = [0]*4
    i = 0
    timenow = time.time()

    while (self._ser.inWaiting() < 4):
      time.sleep(0.01)
      if ((time.time() - timenow) > 1):
        break

    rlt = self._ser.read(self._ser.inWaiting())
    #print(rlt)

    index = len(rlt)
    if(len(rlt) >= 4):
       index = len(rlt) - 4
       while True:
         try:
           data[0] = ord(rlt[index])
         except:
           data[0] = rlt[index]
         if(data[0] == 0xFF):
           break
         elif (index > 0):
           index = index - 1
         else:
           break
       #print(data)
       if (data[0] == 0xFF):
         try:
           data[1] = ord(rlt[index + 1])
           data[2] = ord(rlt[index + 2])
           data[3] = ord(rlt[index + 3])
         except:
           data[1] = rlt[index + 1]
           data[2] = rlt[index + 2]
           data[3] = rlt[index + 3]
         i = 4
    #print(data)
    if i == 4:
      sum = self._check_sum(data)
      if sum != data[3]:
        self.last_operate_status = self.STA_ERR_CHECKSUM
      else:
        self.distance = data[1]*256 + data[2]
        self.last_operate_status = self.STA_OK
      if self.distance > self.distance_max:
        self.last_operate_status = self.STA_ERR_CHECK_OUT_LIMIT
        self.distance = self.distance_max
      elif self.distance < self.distance_min:
        self.last_operate_status = self.STA_ERR_CHECK_LOW_LIMIT
        self.distance = self.distance_min
    else:
      self.last_operate_status = self.STA_ERR_DATA
    return self.distance
DRIVER_EOF

cat > "$SENSOR_DIR/a02yyuw_iot.py" <<'PUBLISH_EOF'
#!/usr/bin/env python3
# Publish A02YYUW distance readings to AWS IoT Core (fixed number of reads)

import os
import sys
import json
import time
import signal
import subprocess
from collections import deque
from datetime import datetime

# ==== Tell Python where the DFRobot lib lives ====
LIB_DIR = os.getenv("A02_LIB_DIR", "/home/adam/DFRobot_RaspberryPi_A02YYUW")
MODULE_FILE = os.path.join(LIB_DIR, "DFRobot_RaspberryPi_A02YYUW.py")
if not os.path.isfile(MODULE_FILE):
    raise SystemExit(
        f"DFRobot lib not found at {MODULE_FILE}\n"
        "Set env var A02_LIB_DIR to the repo path, e.g.:\n"
        "  A02_LIB_DIR=/path/to/DFRobot_RaspberryPi_A02YYUW python3 a02yyuw_iot.py"
    )
sys.path.insert(0, LIB_DIR)

from DFRobot_RaspberryPi_A02YYUW import DFRobot_A02_Distance as Board

# === AWS IoT SDK ===
from awscrt import io, mqtt
from awsiot import mqtt_connection_builder

# ===== AWS IoT config =====
ENDPOINT = os.getenv("AWS_IOT_ENDPOINT", "a2dabndimqd0mb-ats.iot.us-east-1.amazonaws.com")
CERT_PATH = os.getenv("AWS_IOT_CERT", "/home/adam/certs/device.pem.crt")
KEY_PATH = os.getenv("AWS_IOT_KEY", "/home/adam/certs/private.pem.key")
CA_PATH = os.getenv("AWS_IOT_CA", "/home/adam/certs/Amazon-root-CA-1.pem")
TOPIC = os.getenv("AWS_IOT_TOPIC", "pi/TestSensor/telemetry")
DEVICE_ID = os.getenv("DEVICE_ID", "TestSensor")
# The device's IoT policy scopes iot:Connect to
# arn:...:client/${iot:Connection.Thing.ThingName} -- AWS IoT requires the MQTT
# client ID to match the Thing name EXACTLY, or the CONNECT is silently
# rejected (looks like a bare connection-closed error client-side, not an
# auth error). TOPIC is always "pi/<thing-name>/telemetry", so pull the thing
# name back out of it rather than needing yet another env var kept in sync.
try:
    THING_NAME = TOPIC.split("/")[1]
except IndexError:
    THING_NAME = DEVICE_ID

# Cadence & count
INTERVAL = float(os.getenv("READ_INTERVAL_SEC", "5.0"))  # time between publishes
READ_COUNT = int(os.getenv("READ_COUNT", "5"))           # total readings before exit
QOS = mqtt.QoS.AT_LEAST_ONCE
SMOOTH_WINDOW = int(os.getenv("SMOOTH_WINDOW", "5"))

# ===== Sensor setup =====
board = Board()
board.set_dis_range(0, 4500)  # mm

def read_distance_cm():
    dist_mm = board.getDistance()
    status = board.last_operate_status
    if status == board.STA_OK:
        return round(dist_mm / 10.0, 1), status
    else:
        return None, status

# ===== Get temperature =====
def get_cpu_temp_c():
    """
    Returns float temperature in C if available, else None.
    Uses 'vcgencmd measure_temp' which typically outputs: temp=45.2'C
    """
    try:
        out = subprocess.check_output(["vcgencmd", "measure_temp"], timeout=1.5).decode().strip()
        # parse: temp=45.2'C
        if out.startswith("temp=") and out.endswith("'C"):
            val = out.split("=")[1].split("'")[0]
            return round(float(val), 1)
    except Exception:
        pass
    return None

# ===== MQTT connection bootstrap =====
event_loop_group = io.EventLoopGroup(1)
host_resolver = io.DefaultHostResolver(event_loop_group)
client_bootstrap = io.ClientBootstrap(event_loop_group, host_resolver)

mqtt_connection = mqtt_connection_builder.mtls_from_path(
    endpoint=ENDPOINT,
    cert_filepath=CERT_PATH,
    pri_key_filepath=KEY_PATH,
    ca_filepath=CA_PATH,
    client_id=THING_NAME,
    clean_session=False,
    keep_alive_secs=30,
    client_bootstrap=client_bootstrap
)

def shutdown(*_):
    try:
        mqtt_connection.disconnect().result(timeout=5)
    except Exception:
        pass
    sys.exit(0)

signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)

print(f"Connecting to AWS IoT at {ENDPOINT} ...")
mqtt_connection.connect().result()
print("Connected.")

window = deque(maxlen=SMOOTH_WINDOW)

print(f"Publishing {READ_COUNT} reading(s) every {INTERVAL:.1f}s to '{TOPIC}' (QoS 1).")
try:
    for i in range(READ_COUNT):
        t0 = time.time()
        dist_cm, status = read_distance_cm()
        ts = datetime.now().strftime("%Y-%m-%dT%H:%M:%S%z")

        temp_c = get_cpu_temp_c()
        payload = {
            "device": DEVICE_ID,
            "sensor": "A02YYUW",
            "unit": "cm",
            "distance_cm": dist_cm,   # can be None on error
            "status": int(status),
            "ts": ts
        }
        if temp_c is not None:
            payload["temperature_c"] = temp_c

        if dist_cm is not None:
            window.append(dist_cm)
            if len(window) >= 3:
                payload["distance_cm_filtered"] = sorted(window)[len(window)//2]

        try:
            mqtt_connection.publish(topic=TOPIC, payload=json.dumps(payload), qos=QOS)
            print(f"[{i+1}/{READ_COUNT}] Published: {payload}")
        except Exception as e:
            print(f"[{i+1}/{READ_COUNT}] Publish failed: {e}")

        # Maintain steady cadence
        elapsed = time.time() - t0
        sleep_remaining = INTERVAL - elapsed
        if i < READ_COUNT - 1 and sleep_remaining > 0:
            time.sleep(sleep_remaining)
finally:
    print("Disconnecting MQTT ...")
    try:
        mqtt_connection.disconnect().result(timeout=5)
    except Exception:
        pass
    print("Done.")
PUBLISH_EOF

chmod +x "$SENSOR_DIR/a02yyuw_iot.py"
mkdir -p "$CERT_DIR"
chown -R "$TARGET_USER":"$TARGET_USER" "$CERT_DIR" "$SENSOR_DIR" 2>/dev/null || true
usermod -aG dialout "$TARGET_USER" 2>/dev/null || true

# ---------------------------------------------------------------------------
# 5b. Comitup callback: on CONNECTED (wifi joined, whether by captive portal
#     or reconnecting to an already-known network), make sure the sensor
#     timer is running. Self-heals if it was ever stopped, including after
#     a plain reboot -- harmless no-op if there's no cert yet.
# ---------------------------------------------------------------------------
log "Deploying the comitup connect callback..."
cat > "$SENSOR_DIR/comitup-callback.sh" <<CALLBACK_EOF
#!/usr/bin/env bash
#
# Registered with comitup as external_callback (see /etc/comitup.conf).
# comitup calls this with one argument -- HOTSPOT, CONNECTING, or CONNECTED --
# every time its state changes.

STATE="\$1"
CERT_FILE="$CERT_DIR/device.pem.crt"

logger -t comitup-callback "state=\$STATE"

if [ "\$STATE" = "CONNECTED" ] && [ -f "\$CERT_FILE" ]; then
  systemctl start brine-sensor.timer 2>/dev/null || true
fi
CALLBACK_EOF
chmod +x "$SENSOR_DIR/comitup-callback.sh"

# ---------------------------------------------------------------------------
# 6. Per-device environment file. DEVICE_ID / AWS_IOT_TOPIC are placeholders
#    -- onboard-device.sh fills these in for real once this device is
#    registered with AWS IoT (see the AWS IoT onboarding runbook).
#    AWS_IOT_ENDPOINT is account-wide, so it's safe to bake in here.
# ---------------------------------------------------------------------------
if [ ! -f "$SENSOR_DIR/device.env" ]; then
  log "Writing placeholder device.env..."
  cat > "$SENSOR_DIR/device.env" <<EOF
DEVICE_ID=CHANGE_ME
AWS_IOT_TOPIC=pi/CHANGE_ME/telemetry
AWS_IOT_ENDPOINT=a2dabndimqd0mb-ats.iot.us-east-1.amazonaws.com
AWS_IOT_CERT=$CERT_DIR/device.pem.crt
AWS_IOT_KEY=$CERT_DIR/private.pem.key
AWS_IOT_CA=$CERT_DIR/Amazon-root-CA-1.pem
A02_LIB_DIR=$SENSOR_DIR/DFRobot_RaspberryPi_A02YYUW
READ_INTERVAL_SEC=5
READ_COUNT=5
SMOOTH_WINDOW=5
EOF
fi

# ---------------------------------------------------------------------------
# 7. systemd service + timer -- replaces the crontab entry. Oneshot service,
#    fired on a timer, so it shows up properly in `systemctl status` /
#    `journalctl` instead of a silent cron log.
#
#    ExecCondition makes "onboarded" a hard requirement rather than a
#    documentation note: if there's no certificate yet, systemd skips the
#    run (logged as "condition not met", not a failure) instead of
#    publishing with placeholder values. The timer can safely stay enabled
#    across every reboot -- it just won't do anything until onboard-device.sh
#    has run.
# ---------------------------------------------------------------------------
log "Installing brine-sensor systemd service + timer..."
cat > /etc/systemd/system/brine-sensor.service <<EOF
[Unit]
Description=Publish brine tank distance reading to AWS IoT Core
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
ExecCondition=/usr/bin/test -f $CERT_DIR/device.pem.crt
EnvironmentFile=$SENSOR_DIR/device.env
ExecStart=/usr/bin/python3 $SENSOR_DIR/a02yyuw_iot.py
EOF

cat > /etc/systemd/system/brine-sensor.timer <<'EOF'
[Unit]
Description=Run brine-sensor.service on a schedule

[Timer]
OnBootSec=2min
OnUnitActiveSec=1h
AccuracySec=30s
Persistent=true

[Install]
WantedBy=timers.target
EOF

systemctl daemon-reload
systemctl enable brine-sensor.timer
log "brine-sensor.timer enabled -- guarded by ExecCondition, so it's a no-op until onboard-device.sh has run."

# ---------------------------------------------------------------------------
# 7b. Auto-hostname from the Pi's own CPU serial -- no customer-facing naming
#     step at all. golden.img is identical on every SD card; each physical
#     Pi's /proc/cpuinfo Serial is unique hardware, burned in at manufacture,
#     so this produces a different (and stable) hostname on every device with
#     zero user interaction. Runs on every boot rather than just the first --
#     cheap to check, self-heals if anything ever reset it, and idempotent
#     (no-ops immediately if the hostname already matches).
# ---------------------------------------------------------------------------
log "Deploying auto-hostname-from-serial script..."
cat > "$SENSOR_DIR/set-hostname-from-serial.sh" <<'HOSTNAME_EOF'
#!/bin/sh
set -e

CURRENT="$(hostname)"
SERIAL="$(awk -F': ' '/^Serial/ {print $2}' /proc/cpuinfo | tr -d '[:space:]')"

if [ -z "$SERIAL" ] || [ "$SERIAL" = "0000000000000000" ]; then
  logger -t set-hostname-from-serial "WARNING: couldn't read a usable CPU serial from /proc/cpuinfo -- leaving hostname as '$CURRENT'"
  exit 0
fi

DESIRED="brine-$SERIAL"

if [ "$CURRENT" = "$DESIRED" ]; then
  exit 0
fi

hostnamectl set-hostname "$DESIRED"
if grep -qE '^127\.0\.1\.1[[:space:]]' /etc/hosts; then
  sed -i -E "s/^(127\.0\.1\.1[[:space:]]+).*/\1$DESIRED/" /etc/hosts
else
  echo "127.0.1.1	$DESIRED" >> /etc/hosts
fi
logger -t set-hostname-from-serial "Hostname set to $DESIRED (was $CURRENT)"
HOSTNAME_EOF
chmod +x "$SENSOR_DIR/set-hostname-from-serial.sh"

cat > /etc/systemd/system/set-hostname-from-serial.service <<EOF
[Unit]
Description=Set this device's hostname from its unique CPU serial number
After=local-fs.target
Before=network-online.target avahi-daemon.service

[Service]
Type=oneshot
RemainAfterExit=yes
ExecStart=$SENSOR_DIR/set-hostname-from-serial.sh

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable set-hostname-from-serial.service
log "set-hostname-from-serial.service enabled -- each device names itself (brine-<cpu-serial>) on first boot, no customer interaction needed."

# ---------------------------------------------------------------------------
# 8. Nightly reboot -- found in testing that the A02YYUW's readings drift
#    inaccurate after a few days of continuous uptime; a nightly reboot fixed
#    it. Same systemd-timer pattern as the sensor job rather than an actual
#    cron entry, so it shows up in `systemctl list-timers` / `journalctl`
#    like everything else in this image.
# ---------------------------------------------------------------------------
log "Installing nightly reboot timer..."
cat > /etc/systemd/system/nightly-reboot.service <<'EOF'
[Unit]
Description=Nightly reboot (works around A02YYUW readings drifting after a few days of uptime)

[Service]
Type=oneshot
ExecStart=/sbin/reboot
EOF

cat > /etc/systemd/system/nightly-reboot.timer <<'EOF'
[Unit]
Description=Run nightly-reboot.service once a day

[Timer]
OnCalendar=*-*-* 03:00:00
RandomizedDelaySec=15min
Persistent=true

[Install]
WantedBy=timers.target
EOF

systemctl daemon-reload
systemctl enable nightly-reboot.timer
log "nightly-reboot.timer enabled -- reboots daily around 03:00 system time (+/- up to 15min)."
log "NOTE: that's 03:00 in whatever timezone this Pi is set to -- Raspberry Pi OS defaults to UTC"
log "      unless you set one (raspi-config, or Imager Customisation on a stock image). Check with"
log "      'timedatectl' if you want the reboot to land at 3am *your* local time specifically."

log "=== provision-image.sh complete ==="
log "Next: verify comitup + the sensor (see the runbook), then run sanitize-for-imaging.sh before capturing the SD card."
