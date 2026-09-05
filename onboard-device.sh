#!/usr/bin/env bash
#
# onboard-device.sh
#
# Run this FROM YOUR LAPTOP (where your real AWS credentials live) to bring
# a freshly-flashed Raspberry Pi onto the fleet. It:
#   1. Calls the AWS IoT control plane for a short-lived (5 min) claim
#      certificate, scoped to nothing but the provisioning handshake.
#   2. Copies that claim cert + onboard_device.py to the Pi over SSH.
#   3. Runs onboard_device.py on the Pi, which trades the claim for a
#      permanent, device-specific certificate via the fleet provisioning
#      template.
#   4. Wipes the temporary claim material from both machines.
#   5. Points the sensor's device.env at this device's identity and starts
#      the brine-sensor timer, so it picks up the new cert immediately.
#
# Assumes the Pi was flashed from the golden image (see the wifi/sensor
# provisioning runbook) -- i.e. /opt/brine-sensor/device.env and the
# brine-sensor.timer unit already exist, just with placeholder values, AND
# that it's already on your wifi network. The device names itself on its own
# first boot (brine-<its-cpu-serial>, via set-hostname-from-serial.service --
# no customer-facing naming step, nothing for you to set by hand either). To
# find a specific device's hostname to target below, check your router's
# DHCP client list for the newest entry starting with "brine-". That
# hostname -- not a value typed here -- is what becomes the device's AWS IoT
# device ID, so it stays the one identifier used everywhere (hostname, thing
# name, MQTT topic, and whatever your Lambda keys the DynamoDB row on).
#
# Requires locally: aws cli (configured with a principal allowed to call
# iot:CreateProvisioningClaim on the template below), jq, ssh, scp.
#
# Usage:
#   ./onboard-device.sh <pi-ssh-target> [template-name] [aws-region]
#
# Example:
#   ./onboard-device.sh pi@brine-1000000012345678.local

set -euo pipefail

PI_TARGET="${1:?Usage: $0 <pi-ssh-target> [template-name] [aws-region]}"
TEMPLATE_NAME="${2:-BrineTankProvisioning}"
AWS_REGION="${3:-$(aws configure get region)}"

# Where the sensor app expects its certs, its config file, and the timer
# unit that runs it. These match provision-image.sh's defaults -- adjust if
# you customized TARGET_USER or SENSOR_DIR there.
REMOTE_CERT_DIR="/home/pi/certs"
SENSOR_DIR="/opt/brine-sensor"
REMOTE_TIMER_NAME="brine-sensor.timer"

ROOT_CA_URL="https://www.amazontrust.com/repository/AmazonRootCA1.pem"
LOCAL_TMP="$(mktemp -d)"
REMOTE_TMP="provision-tmp-$$"
trap 'rm -rf "$LOCAL_TMP"' EXIT

echo "==> Reading this device's hostname..."
DEVICE_ID="$(ssh "$PI_TARGET" "hostname" | tr -d '[:space:]')"
if [ -z "$DEVICE_ID" ]; then
  echo "ERROR: couldn't read a hostname from $PI_TARGET." >&2
  exit 1
fi
if [ "$DEVICE_ID" = "raspberrypi" ]; then
  echo "ERROR: $PI_TARGET's hostname is still the generic default ('raspberrypi')." >&2
  echo "       Every device should auto-name itself (brine-<cpu-serial>) on first boot via" >&2
  echo "       set-hostname-from-serial.service, so this means that didn't happen. Check it:" >&2
  echo "         ssh $PI_TARGET systemctl status set-hostname-from-serial.service" >&2
  echo "         ssh $PI_TARGET journalctl -u set-hostname-from-serial.service" >&2
  echo "       If it's genuinely broken (couldn't read /proc/cpuinfo, say), you can force a" >&2
  echo "       name by hand instead:" >&2
  echo "         ssh $PI_TARGET sudo raspi-config nonint do_hostname <new-name>" >&2
  echo "         ssh $PI_TARGET sudo reboot" >&2
  echo "       then re-run this against the new hostname." >&2
  exit 1
fi
echo "    $DEVICE_ID"

echo "==> Fetching the AWS IoT endpoint for this account..."
IOT_ENDPOINT="$(aws iot describe-endpoint --endpoint-type iot:Data-ATS --region "$AWS_REGION" --query endpointAddress --output text)"
echo "    $IOT_ENDPOINT"

echo "==> Requesting a 5-minute provisioning claim for template '$TEMPLATE_NAME'..."
CLAIM_JSON="$(aws iot create-provisioning-claim --template-name "$TEMPLATE_NAME" --region "$AWS_REGION")"

echo "$CLAIM_JSON" | jq -r '.certificatePem' > "$LOCAL_TMP/claim.pem.crt"
echo "$CLAIM_JSON" | jq -r '.keyPair.PrivateKey' > "$LOCAL_TMP/claim.pem.key"
chmod 600 "$LOCAL_TMP/claim.pem.key"

if [ ! -f "$LOCAL_TMP/AmazonRootCA1.pem" ]; then
  curl -fsSL "$ROOT_CA_URL" -o "$LOCAL_TMP/AmazonRootCA1.pem"
fi

echo "==> Copying claim material + provisioning script to $PI_TARGET..."
ssh "$PI_TARGET" "mkdir -p ~/$REMOTE_TMP"
scp -q "$LOCAL_TMP/claim.pem.crt" "$LOCAL_TMP/claim.pem.key" "$LOCAL_TMP/AmazonRootCA1.pem" \
    "$(dirname "$0")/onboard_device.py" \
    "$PI_TARGET:~/$REMOTE_TMP/"

echo "==> Running the fleet-provisioning handshake on the Pi..."
REMOTE_OUTPUT="$(ssh "$PI_TARGET" "
  set -e
  cd ~/$REMOTE_TMP
  python3 -c 'import awsiot' 2>/dev/null || pip3 install --break-system-packages -q awsiotsdk
  python3 onboard_device.py \
    --endpoint '$IOT_ENDPOINT' \
    --claim-cert claim.pem.crt \
    --claim-key claim.pem.key \
    --root-ca AmazonRootCA1.pem \
    --template-name '$TEMPLATE_NAME' \
    --serial-number '$DEVICE_ID' \
    --out-dir '$REMOTE_CERT_DIR'
")"

echo "$REMOTE_OUTPUT"
THING_NAME="$(echo "$REMOTE_OUTPUT" | grep '^THING_NAME=' | cut -d= -f2)"
# Must match the ${iot:Connection.Thing.ThingName} the production IoT policy
# scopes publish permission to -- so the topic uses the full thing name
# (with its BrineTank- prefix), not the bare device id.
AWS_IOT_TOPIC="pi/$THING_NAME/telemetry"

echo "==> Pointing this device's config at $THING_NAME and starting the sensor timer..."
ssh "$PI_TARGET" "
  rm -rf ~/$REMOTE_TMP
  sudo sed -i \
    -e 's/^DEVICE_ID=.*/DEVICE_ID=$DEVICE_ID/' \
    -e 's#^AWS_IOT_TOPIC=.*#AWS_IOT_TOPIC=$AWS_IOT_TOPIC#' \
    -e 's/^AWS_IOT_ENDPOINT=.*/AWS_IOT_ENDPOINT=$IOT_ENDPOINT/' \
    '$SENSOR_DIR/device.env'
  sudo systemctl restart $REMOTE_TIMER_NAME 2>/dev/null || echo 'NOTE: could not start $REMOTE_TIMER_NAME automatically -- start it manually.'
"

echo ""
echo "Done. $DEVICE_ID is now provisioned as AWS IoT thing: $THING_NAME"
echo "Publishing to topic: $AWS_IOT_TOPIC"
echo "Verify with: aws iot describe-thing --thing-name $THING_NAME --region $AWS_REGION"
