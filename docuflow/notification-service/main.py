import os
import json
import asyncio
import logging
import aiohttp
import httpx

from azure.identity.aio import DefaultAzureCredential
from azure.servicebus.aio import ServiceBusClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SB_FQDN = os.getenv("SERVICEBUS_FQDN")
SB_QUEUE = os.getenv("SERVICEBUS_NOTIFY_QUEUE", "doc-notify-queue")
WEBHOOK = os.getenv("NOTIFY_WEBHOOK_URL")

cred = DefaultAzureCredential()

async def run():
    async with ServiceBusClient(
        fully_qualified_namespace=SB_FQDN,
        credential=cred
    ) as client:

        receiver = client.get_queue_receiver(
            queue_name=SB_QUEUE,
            max_wait_time=30
        )

        async with receiver:
            async for msg in receiver:
                try:
                    body = b"".join([
                        b for b in msg.body
                    ]).decode("utf-8")

                    data = json.loads(body)

                    async with httpx.AsyncClient(timeout=10) as http:
                        response = await http.post(
                            WEBHOOK,
                            json=data
                        )

                        response.raise_for_status()

                    await receiver.complete_message(msg)

                    logger.info("Notification delivered")

                except Exception as e:
                    logger.exception(f"Notification failed: {e}")

                    await receiver.dead_letter_message(
                        msg,
                        reason="notification_delivery_failed"
                    )

if __name__ == "__main__":
    asyncio.run(run())
