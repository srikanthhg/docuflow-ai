import os
import json
import logging

from fastapi import FastAPI, HTTPException

from databricks import sql
from azure.identity import DefaultAzureCredential

app = FastAPI(title="DocuFlow Query Service")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DB_HOST = os.getenv("DATABRICKS_SQL_HOST")
DB_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")

# Azure Databricks Resource ID
DATABRICKS_RESOURCE_ID = os.getenv("DATABRICKS_RESOURCE_ID")
DATABRICKS_CATALOG = os.getenv("DATABRICKS_CATALOG", "docflow")
DATABRICKS_SCHEMA = os.getenv("DATABRICKS_SCHEMA", "extraction")

credential = DefaultAzureCredential()

def get_db_connection():
    token = credential.get_token(
        f"{DATABRICKS_RESOURCE_ID}/.default"
    ).token

    return sql.connect(
        server_hostname=DB_HOST,
        http_path=DB_HTTP_PATH,
        access_token=token,
        catalog=DATABRICKS_CATALOG,
        schema=DATABRICKS_SCHEMA,
        _user_agent_entry="docuflow-query-service/1.0"
    )

@app.get("/api/v1/query/{doc_id}")
async def get_document(doc_id: str):
    conn = None

    try:
        conn = get_db_connection()

        cur = conn.cursor()

        cur.execute(
            """
            SELECT
                doc_id,
                extracted_json,
                model_version,
                processed_at
            FROM docflow.extraction_results
            WHERE doc_id = ?
            """,
            (doc_id,)
        )

        row = cur.fetchone()

        if not row:
            raise HTTPException(
                status_code=404,
                detail="Document not found"
            )

        return {
            "doc_id": row[0],
            "data": json.loads(row[1]),
            "model_version": row[2],
            "processed_at": str(row[3])
        }

    except HTTPException:
        raise

    except Exception as e:
        logger.exception(f"Query failure: {e}")

        raise HTTPException(
            status_code=500,
            detail="Query failed"
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
