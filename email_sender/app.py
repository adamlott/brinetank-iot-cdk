import os, json, boto3, logging

# Initialize clients at module level (reused across invocations)
ses = boto3.client("sesv2")
log = logging.getLogger()
log.setLevel(logging.INFO)

# Environment variables
SES_FROM = os.environ["SES_FROM"]


def _send_email(subject: str, body_text: str, to_addrs: list[str]) -> None:
    """
    Send a plain-text email via SES v2.

    Args:
        subject: Email subject line
        body_text: Plain-text email body
        to_addrs: List of recipient email addresses
    """
    log.info(f"Sending email to {to_addrs}, subject: {subject}")
    response = ses.send_email(
        FromEmailAddress=SES_FROM,
        Destination={"ToAddresses": to_addrs},
        Content={
            "Simple": {
                "Subject": {"Data": subject},
                "Body": {"Text": {"Data": body_text}},
            }
        },
    )
    log.info(f"SES send_email response: {json.dumps(response, default=str)}")


def handler(event, context):
    """
    Lambda entry point — processes SQS records and sends emails via SES.

    Each SQS message body is expected to be a JSON object with:
        - subject (str): Email subject line
        - body (str): Email body text
        - to_addrs (list[str]): Recipient addresses

    Failed records are returned in batchItemFailures so SQS can retry
    individual messages without re-processing the whole batch.
    """
    log.info(f"Processing {len(event.get('Records', []))} SQS record(s)")
    batch_item_failures = []

    for record in event.get("Records", []):
        message_id = record.get("messageId", "unknown")
        try:
            body = json.loads(record["body"])
            subject = body["subject"]
            body_text = body["body"]
            to_addrs = body["to_addrs"]

            if not to_addrs:
                log.warning(f"[{message_id}] to_addrs is empty — skipping send")
                continue

            _send_email(subject, body_text, to_addrs)
            log.info(f"[{message_id}] Email sent successfully")

        except Exception as e:
            log.error(f"[{message_id}] Failed to process SQS record: {e}")
            log.error(f"[{message_id}] Record body: {record.get('body', '')}")
            batch_item_failures.append({"itemIdentifier": message_id})

    if batch_item_failures:
        log.warning(f"{len(batch_item_failures)} record(s) failed; returning for SQS retry")

    return {"batchItemFailures": batch_item_failures}
