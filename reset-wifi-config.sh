#!/usr/bin/env bash
#
# reset-wifi-for-delivery.sh
#
# Run this ONCE, by hand, on a device that's already been provisioned,
# verified, and onboarded to AWS IoT (i.e. onboard-device.sh has completed)
# -- right before you power it off to carry it to the customer's house and
# install it at the tank.
#
# THIS IS NOT sanitize-for-imaging.sh. That script wipes a reference Pi back
# to a totally clean slate -- SSH host keys, machine-id, hostname, AWS
# certs, device.env, all of it -- so it's safe to re-clone as golden.img.
# Running it on a customer's device would destroy that device's real AWS
# IoT identity and require re-onboarding.
#
# This script does the opposite, narrower job: it keeps this specific
# device's identity exactly as-is -- its hostname, its AWS IoT certificate,
# its device.env -- since that's the real, already-registered
# BrineTank-<id> this customer's readings will show up under. It only
# clears the one thing that's wrong for the device's next stop: the wifi
# network it learned here in your shop/office. Without this,
# comitup won't broadcast its setup AP at the customer's house at all --
# per its own design, it only opens the fallback AP when it has NO known
# network to try, and right now it has one (yours), just one that won't be
# in range anymore. Left alone, the device would just sit there silently
# trying to reach a network it can never find.
#
# Safe to run any time after onboard-device.sh has completed, and safe to
# re-run (each run just clears whatever wifi network is currently saved).

set -euo pipefail

if [ "$(id -u)" -ne 0 ]; then
  echo "Run this as root: sudo ./reset-wifi-config.sh" >&2
  exit 1
fi

echo "Stopping the sensor timer (nothing to publish to until it's reconnected at the customer's)..."
systemctl stop brine-sensor.timer 2>/dev/null || true

echo "Clearing the wifi network this device learned here..."
rm -f /etc/NetworkManager/system-connections/*.nmconnection

echo "Restarting comitup so you can confirm it's broadcasting again before you leave..."
systemctl restart comitup

echo ""
echo "Done. This device kept its identity (hostname, AWS IoT certificate, device.env) --"
echo "only the wifi network was cleared. Check your phone's wifi list now: you should see"
echo "a comitup-<nnn> network within a few seconds. If it's there, you're good to power"
echo "off and go -- the customer joins that same AP at their house and picks their own"
echo "network, exactly like first-time setup on a brand new device."
echo ""
echo "Once it reconnects at the customer's, the sensor timer restarts automatically via"
echo "comitup-callback.sh -- nothing left for you (or them) to do by hand."
