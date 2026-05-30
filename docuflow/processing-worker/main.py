import os
import json
import asyncio
import logging
import io
import httpx
from typing import Dict, Any, Optional, List

from azure.identity.aio import (
    DefaultAzureCredential,
    get_bearer_token_provider
)
from azure.servicebus.aio import ServiceBusClient
from azure.storage.blob.aio import BlobServiceClient
from openai import AzureOpenAI
from pydantic import BaseModel, Field
from pypdf import PdfReader
import aiohttp

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BLOB_URL = os.getenv("AZURE_STORAGE_BLOB_URL")
BLOB_CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "doc-uploads")
SB_FQDN = os.getenv("SERVICEBUS_FQDN")
SB_QUEUE = os.getenv("SERVICEBUS_QUEUE", "doc-ingest-queue")
AOAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT")
AOAI_DEPLOY = os.getenv("AZURE_OPENAI_DEPLOYMENT", "gpt-5-mini")
STORAGE_SVC_URL = os.getenv(
    "STORAGE_SERVICE_URL",
    "http://storage-service:8000/api/v1/storage/documents"
)

cred = DefaultAzureCredential()

class DocumentSchema(BaseModel):
    document_type: str
    extracted_fields: Dict[str, Any]
    summary: str
    confidence_score: float = Field(ge=0.0, le=1.0)
    warnings: Optional[List[str]] = None

async def download_and_extract(blob_path: str) -> str:
    async with BlobServiceClient(
        account_url=BLOB_URL,
        credential=cred
    ) as client:
        container = client.get_container_client(BLOB_CONTAINER)

        stream = await container.download_blob(blob_path)
        content = await stream.readall()

    pdf = PdfReader(io.BytesIO(content))

    extracted_text = []

    for page in pdf.pages:
        extracted_text.append(page.extract_text() or "")

    return "\n".join(extracted_text)

async def call_openai(text: str) -> dict:
    token_provider = get_bearer_token_provider(
        cred,
        "https://cognitiveservices.azure.com/.default"
    )

    client = AzureOpenAI(
        azure_endpoint=AOAI_ENDPOINT,
        azure_ad_token_provider=token_provider,
        api_version="2024-08-01-preview"
    )

    schema = DocumentSchema.model_json_schema()

    response = client.chat.completions.create(
        model=AOAI_DEPLOY,
        messages=[
            {
                "role": "system",
                "content": "Extract structured data and return valid JSON only."
            },
            {
                "role": "user",
                "content": text[:120000]
            }
        ],
        response_format={
            "type": "json_schema",
            "json_schema": {
                "name": "DocExtract",
                "schema": schema,
                "strict": True
            }
        },
        temperature=0.0,
        max_tokens=4096
    )

    return DocumentSchema.model_validate_json(
        response.choices[0].message.content
    ).model_dump()

async def process_message(payload: dict):
    doc_id = payload["doc_id"]
    blob_path = payload["blob_path"]

    text = await download_and_extract(blob_path)

    if not text.strip():
        raise ValueError("No extractable text found")

    result = await call_openai(text)

    async with httpx.AsyncClient(timeout=30) as http:
        response = await http.post(
            STORAGE_SVC_URL,
            json={
                "doc_id": doc_id,
                "extracted_data": result,
                "model_version": AOAI_DEPLOY
            }
        )

        response.raise_for_status()

    logger.info(f"Stored results for {doc_id}")

async def run_worker():
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

                    payload = json.loads(body)

                    await process_message(payload)

                    await receiver.complete_message(msg)

                    logger.info("Message completed")

                except Exception as e:
                    logger.exception(f"Worker failed: {e}")

                    await receiver.abandon_message(msg)

if __name__ == "__main__":
    asyncio.run(run_worker())
