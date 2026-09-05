#!/usr/bin/env bash
#
# sanitize-for-imaging.sh
#
# Run this ONCE, by hand, on the reference Pi -- after provision-image.sh has
# finished and you've verified comitup + the sensor work -- right before you
# power it off to capture the SD card as golden.img.
#
# Without this, every device cloned from that image would share the same
# SSH host keys, the same machine-id, and (if you tested onboarding on this
# reference Pi) the same AWS IoT certificate -- all of which need to be
# unique per device. (Hostname isn't really in that list either -- each
# device now sets its own from its unique CPU serial on first boot via
# set-hostname-from-serial.service, regardless of what's baked into the
# image -- so it doesn't matter what this reference Pi is called when you
# capture it. This script still resets it to the generic default below, as
# a harmless backstop in case that service ever fails to run.)

set -euo pipefail

if [ "$(id -u)" -ne 0 ]; then
  echo "Run this as root: sudo ./sanitize-for-imaging.sh" >&2
  exit 1
fi

SENSOR_DIR="/opt/brine-sensor"

echo "Stopping the sensor timer..."
systemctl stop brine-sensor.timer 2>/dev/null || true

echo "Resetting hostname to the generic default..."
hostnamectl set-hostname raspberrypi
sed -i -E 's/^(127\.0\.1\.1\s+).*/\1raspberrypi/' /etc/hosts

echo "Re-arming host key regeneration for the next device's first boot..."
# regenerate_ssh_host_keys.service is a one-shot: it runs once, on an image's
# very first boot, then disables ITSELF (see its own script -- ends with
# `systemctl -q disable regenerate_ssh_host_keys`). That already fired for
# real back when you first booted this reference Pi in Phase A step 2 -- so
# by now it's a spent match. If we just delete the host keys below without
# re-enabling this first, golden.img ships with no keys AND no mechanism left
# to create new ones, and every device cloned from it fails to start sshd at
# all (nothing to listen on port 22 -- looks like "connection refused", not a
# network problem). Re-enabling it here means it fires again, for real, on
# each new device's actual first boot -- then disables itself again there too.
systemctl enable regenerate_ssh_host_keys.service

echo "Clearing SSH host keys (regenerate_ssh_host_keys.service recreates them on next boot)..."
rm -f /etc/ssh/ssh_host_*

echo "Clearing machine-id (regenerated on next boot)..."
truncate -s 0 /etc/machine-id
rm -f /var/lib/dbus/machine-id
ln -sf /etc/machine-id /var/lib/dbus/machine-id

echo "Clearing any wifi networks comitup/NetworkManager learned during testing..."
rm -f /etc/NetworkManager/system-connections/*.nmconnection

echo "Wiping any AWS IoT certificate used for testing..."
rm -f /home/*/certs/device.pem.crt /home/*/certs/private.pem.key /home/*/certs/Amazon-root-CA-1.pem

echo "Resetting device.env to placeholders..."
if [ -f "$SENSOR_DIR/device.env" ]; then
  sed -i \
    -e 's/^DEVICE_ID=.*/DEVICE_ID=CHANGE_ME/' \
    -e 's#^AWS_IOT_TOPIC=.*#AWS_IOT_TOPIC=pi/CHANGE_ME/telemetry#' \
    "$SENSOR_DIR/device.env"
fi

echo "Clearing shell history..."
rm -f /root/.bash_history
for h in /home/*/.bash_history; do rm -f "$h" 2>/dev/null || true; done

echo ""
echo "Done. Now: sudo poweroff"
echo "Then read the SD card back into golden.img (Win32DiskImager, or dd on Linux/WSL)."
