import os
import uuid
import json
import logging
import aiohttp
from fastapi import FastAPI, File, UploadFile, HTTPException
from pydantic import BaseModel
from azure.storage.blob.aio import BlobServiceClient
from azure.servicebus.aio import ServiceBusClient
from azure.servicebus import ServiceBusMessage
from azure.identity.aio import DefaultAzureCredential

app = FastAPI(title="DocuFlow Ingestion Service")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

BLOB_URL = os.getenv("AZURE_STORAGE_BLOB_URL")
BLOB_CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "doc-uploads")
SB_FQDN = os.getenv("SERVICEBUS_FQDN")
SB_QUEUE = os.getenv("SERVICEBUS_QUEUE", "doc-ingest-queue")
MAX_FILE_SIZE_MB = int(os.getenv("MAX_FILE_SIZE_MB", "50"))

cred = DefaultAzureCredential()

class IngestionResponse(BaseModel):
    doc_id: str
    status: str
    tracking_url: str

@app.post("/api/v1/ingest", status_code=202, response_model=IngestionResponse)
async def ingest_document(file: UploadFile = File(...)):
    if not file.filename:
        raise HTTPException(status_code=400, detail="Filename is required")

    doc_id = str(uuid.uuid4())
    ext = os.path.splitext(file.filename)[1] or ".pdf"
    blob_path = f"{doc_id}{ext}"

    try:
        content = await file.read()

        if len(content) > MAX_FILE_SIZE_MB * 1024 * 1024:
            raise HTTPException(
                status_code=413,
                detail=f"File exceeds {MAX_FILE_SIZE_MB}MB limit"
            )

        async with BlobServiceClient(
            account_url=BLOB_URL,
            credential=cred
        ) as blob_client:
            container_client = blob_client.get_container_client(BLOB_CONTAINER)

            await container_client.upload_blob(
                name=blob_path,
                data=content,
                overwrite=True
            )

        message = {
            "doc_id": doc_id,
            "blob_path": blob_path,
            "filename": file.filename,
            "content_type": file.content_type
        }

        async with ServiceBusClient(
            fully_qualified_namespace=SB_FQDN,
            credential=cred
        ) as sb_client:
            sender = sb_client.get_queue_sender(queue_name=SB_QUEUE)

            async with sender:
                await sender.send_messages(
                    ServiceBusMessage(
                        json.dumps(message),
                        message_id=doc_id
                    )
                )

        logger.info(f"Queued document: {doc_id}")

        return IngestionResponse(
            doc_id=doc_id,
            status="queued",
            tracking_url=f"/api/v1/query/{doc_id}"
        )

    except HTTPException:
        raise

    except Exception as e:
        logger.exception(f"Ingestion failed: {e}")
        raise HTTPException(status_code=500, detail="Document ingestion failed")

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.get("/ready")
async def ready():
    return {"status": "ready"}
