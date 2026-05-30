import os
import json
import logging

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Dict, Any

from databricks import sql
from azure.identity import DefaultAzureCredential

app = FastAPI(title="DocuFlow Storage Service")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DB_HOST = os.getenv("DATABRICKS_SQL_HOST")
DB_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")

# Azure Databricks Resource ID
DATABRICKS_RESOURCE_ID = os.getenv("DATABRICKS_RESOURCE_ID")

credential = DefaultAzureCredential()

class StoragePayload(BaseModel):
    doc_id: str
    extracted_data: Dict[str, Any]
    model_version: str

def get_db_connection():
    token = credential.get_token(
        f"{DATABRICKS_RESOURCE_ID}/.default"
    ).token

    return sql.connect(
        server_hostname=DB_HOST,
        http_path=DB_HTTP_PATH,
        access_token=token
    )

@app.post("/api/v1/storage/documents", status_code=201)
async def upsert_document(payload: StoragePayload):
    conn = None

    try:
        conn = get_db_connection()

        cur = conn.cursor()

        cur.execute(
            """
            MERGE INTO docflow.extraction_results t
            USING (
                SELECT
                    ? AS s_id,
                    ? AS s_json,
                    ? AS s_ver,
                    current_timestamp() AS s_ts
            ) s
            ON t.doc_id = s.s_id

            WHEN MATCHED THEN UPDATE SET
                extracted_json = s.s_json,
                model_version = s.s_ver,
                processed_at = s.s_ts

            WHEN NOT MATCHED THEN INSERT (
                doc_id,
                extracted_json,
                model_version,
                status,
                processed_at
            )
            VALUES (
                s.s_id,
                s.s_json,
                s.s_ver,
                'completed',
                s.s_ts
            )
            """,
            (
                payload.doc_id,
                json.dumps(payload.extracted_data),
                payload.model_version
            )
        )

        conn.commit()

        logger.info(f"Stored document: {payload.doc_id}")

        return {
            "doc_id": payload.doc_id,
            "status": "stored"
        }

    except Exception as e:
        logger.exception(f"Storage failure: {e}")

        raise HTTPException(
            status_code=500,
            detail="Storage operation failed"
        )

    finally:
        if conn:
            conn.close()

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.get("/ready")
async def ready():
    return {"status": "ready"}
