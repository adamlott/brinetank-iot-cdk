#!/usr/bin/env python3
"""
onboard_device.py

Runs ON THE RASPBERRY PI. Performs the AWS IoT "fleet provisioning by
trusted user" MQTT handshake using a short-lived claim certificate, and
writes a permanent, device-specific certificate + key to disk.

This talks to three AWS IoT reserved MQTT topics, in order:
  1. $aws/certificates/create/json            -> get a fresh cert + key
  2. $aws/provisioning-templates/<template>/provision/json
                                               -> register the Thing and
                                                  activate the cert
  3. (implicit) disconnect the claim connection; the new cert is now the
     one this device should use going forward.

Requires: pip3 install awsiotsdk   (AWS IoT Device SDK v2 for Python)

Usage:
  python3 onboard_device.py \
      --endpoint xxxxxxxxxxxxxx-ats.iot.us-east-1.amazonaws.com \
      --claim-cert claim.pem.crt --claim-key claim.pem.key \
      --root-ca AmazonRootCA1.pem \
      --template-name BrineTankProvisioning \
      --serial-number garage-softener-01 \
      --out-dir /home/pi/certs
"""

import argparse
import json
import os
import shutil
import stat
import sys
import uuid
from concurrent.futures import Future

from awscrt import mqtt
from awsiot import mqtt_connection_builder


def parse_args():
    p = argparse.ArgumentParser(description="Fleet-provision this device with AWS IoT Core.")
    p.add_argument("--endpoint", required=True, help="AWS IoT data endpoint, e.g. xxxx-ats.iot.<region>.amazonaws.com")
    p.add_argument("--claim-cert", required=True, help="Path to the temporary claim certificate PEM")
    p.add_argument("--claim-key", required=True, help="Path to the temporary claim private key")
    p.add_argument("--root-ca", required=True, help="Path to AmazonRootCA1.pem")
    p.add_argument("--template-name", required=True, help="Fleet provisioning template name")
    p.add_argument("--serial-number", required=True, help="Device/serial identifier passed to the template")
    p.add_argument("--out-dir", required=True, help="Directory to write the permanent cert/key into")
    p.add_argument("--thing-group", default=None, help="Optional: extra parameter passed to the template (must be declared as a Parameter there)")
    return p.parse_args()


def wait_for(future: Future, label: str, timeout=20):
    try:
        return future.result(timeout=timeout)
    except Exception as e:
        print(f"ERROR: timed out or failed waiting for {label}: {e}", file=sys.stderr)
        sys.exit(1)


def main():
    args = parse_args()
    client_id = f"claim-{args.serial_number}-{uuid.uuid4().hex[:8]}"

    connection = mqtt_connection_builder.mtls_from_path(
        endpoint=args.endpoint,
        cert_filepath=args.claim_cert,
        pri_key_filepath=args.claim_key,
        ca_filepath=args.root_ca,
        client_id=client_id,
        clean_session=True,
        keep_alive_secs=30,
    )

    print(f"Connecting to {args.endpoint} as {client_id} using claim cert...")
    wait_for(connection.connect(), "MQTT connect")

    # ---- Step 1: exchange the claim for a brand-new keypair/cert ----
    create_cert_result = Future()

    def on_create_cert_accepted(topic, payload, **kwargs):
        create_cert_result.set_result(json.loads(payload))

    def on_create_cert_rejected(topic, payload, **kwargs):
        create_cert_result.set_exception(RuntimeError(f"CreateKeysAndCertificate rejected: {payload}"))

    wait_for(connection.subscribe(
        topic="$aws/certificates/create/json/accepted",
        qos=mqtt.QoS.AT_LEAST_ONCE, callback=on_create_cert_accepted)[0], "subscribe create/accepted")
    wait_for(connection.subscribe(
        topic="$aws/certificates/create/json/rejected",
        qos=mqtt.QoS.AT_LEAST_ONCE, callback=on_create_cert_rejected)[0], "subscribe create/rejected")

    wait_for(connection.publish(
        topic="$aws/certificates/create/json", payload="{}",
        qos=mqtt.QoS.AT_LEAST_ONCE)[0], "publish create request")

    cert_response = wait_for(create_cert_result, "new certificate")
    ownership_token = cert_response["certificateOwnershipToken"]
    new_cert_pem = cert_response["certificatePem"]
    new_private_key = cert_response["privateKey"]
    print(f"Received new certificate {cert_response['certificateId'][:12]}...")

    # ---- Step 2: register the Thing using the provisioning template ----
    register_result = Future()

    def on_register_accepted(topic, payload, **kwargs):
        register_result.set_result(json.loads(payload))

    def on_register_rejected(topic, payload, **kwargs):
        register_result.set_exception(RuntimeError(f"RegisterThing rejected: {payload}"))

    provision_topic = f"$aws/provisioning-templates/{args.template_name}/provision/json"
    wait_for(connection.subscribe(
        topic=f"{provision_topic}/accepted",
        qos=mqtt.QoS.AT_LEAST_ONCE, callback=on_register_accepted)[0], "subscribe provision/accepted")
    wait_for(connection.subscribe(
        topic=f"{provision_topic}/rejected",
        qos=mqtt.QoS.AT_LEAST_ONCE, callback=on_register_rejected)[0], "subscribe provision/rejected")

    parameters = {"SerialNumber": args.serial_number}
    if args.thing_group:
        parameters["ThingGroup"] = args.thing_group

    register_payload = json.dumps({
        "certificateOwnershipToken": ownership_token,
        "parameters": parameters,
    })
    wait_for(connection.publish(
        topic=provision_topic, payload=register_payload,
        qos=mqtt.QoS.AT_LEAST_ONCE)[0], "publish register request")

    register_response = wait_for(register_result, "thing registration")
    thing_name = register_response["thingName"]
    print(f"Registered as thing: {thing_name}")

    wait_for(connection.disconnect(), "MQTT disconnect")

    # ---- Step 3: write the permanent cert/key where the sensor service expects them ----
    os.makedirs(args.out_dir, exist_ok=True)
    cert_path = os.path.join(args.out_dir, "device.pem.crt")
    key_path = os.path.join(args.out_dir, "private.pem.key")
    ca_path = os.path.join(args.out_dir, "Amazon-root-CA-1.pem")

    with open(cert_path, "w") as f:
        f.write(new_cert_pem)
    with open(key_path, "w") as f:
        f.write(new_private_key)
    os.chmod(key_path, stat.S_IRUSR | stat.S_IWUSR)  # 0600, private key
    shutil.copyfile(args.root_ca, ca_path)

    print(f"Wrote {cert_path}, {key_path}, {ca_path}")
    # Last line, on its own: the orchestrator script parses this to report success.
    print(f"THING_NAME={thing_name}")


if __name__ == "__main__":
    main()
