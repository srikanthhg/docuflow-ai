import streamlit as st
import httpx
import time
import os
import json

# ✅ MUST be first Streamlit command
st.set_page_config(
    page_title="DocuFlow AI",
    layout="wide",
    page_icon="📄"
)

st.title("📄 DocuFlow AI")
st.success("✅ Application is running!")

# Backend configuration
BACKEND_URL = os.getenv("BACKEND_URL", "https://api.docflow.internal")
st.caption(f"🔗 Backend: `{BACKEND_URL}`")

st.divider()

# File uploader
uploaded_file = st.file_uploader(
    "Upload a document for AI extraction",
    type=["pdf", "png", "jpg", "jpeg", "docx", "txt"],
    help="Supported: PDF, Images, DOCX, TXT"
)

if uploaded_file:
    with st.status("🚀 Processing document...", expanded=True) as status:
        try:
            # Step 1: Upload to ingestion service
            st.write("📤 Uploading document...")
            with httpx.Client(timeout=30) as client:
                resp = client.post(
                    f"{BACKEND_URL}/api/v1/ingest",
                    files={"file": (uploaded_file.name, uploaded_file.read(), uploaded_file.type)},
                    headers={"Accept": "application/json"}
                )
                resp.raise_for_status()
                ingest_data = resp.json()
                doc_id = ingest_data["doc_id"]
                st.success(f"✅ Queued! Document ID: `{doc_id}`")
            
            # Step 2: Poll for results
            st.write("⏳ AI is extracting data (async processing)...")
            results = None
            
            for attempt in range(45):  # ~90 seconds max polling
                time.sleep(2)
                with httpx.Client(timeout=10) as client:
                    query_resp = client.get(f"{BACKEND_URL}/api/v1/query/{doc_id}")
                    
                    if query_resp.status_code == 200:
                        results = query_resp.json()
                        break
                    elif query_resp.status_code == 404:
                        continue  # Still processing
                    else:
                        st.warning(f"⚠️ Query returned {query_resp.status_code}: {query_resp.text}")
                        break
            
            # Step 3: Display results
            if results:
                st.subheader("📊 Extraction Results")
                
                # Display key fields
                if "data" in results:
                    data = results["data"]
                    
                    # Document type
                    if "document_type" in data:
                        st.metric("📋 Document Type", data["document_type"])
                    
                    # Summary
                    if "summary" in data:
                        with st.expander("📝 Summary", expanded=True):
                            st.write(data["summary"])
                    
                    # Extracted fields
                    if "extracted_fields" in data:
                        with st.expander("🔍 Extracted Fields", expanded=True):
                            st.json(data["extracted_fields"])
                    
                    # Confidence
                    if "confidence_score" in data:
                        st.progress(data["confidence_score"])
                        st.caption(f"Confidence: {data['confidence_score']*100:.1f}%")
                
                # Raw JSON for debugging
                with st.expander("🔧 Raw Response"):
                    st.json(results)
                
                status.update(label="✅ Processing Complete!", state="complete", expanded=False)
            else:
                st.info("⏳ Still processing. Check back later or use the query endpoint directly.")
                status.update(label="⏳ Processing...", state="running")
                
        except httpx.ConnectError:
            st.error(f"❌ Cannot connect to backend: `{BACKEND_URL}`")
            st.caption("💡 Ensure AKS services are deployed and Private DNS resolves correctly.")
            status.update(label="❌ Connection Failed", state="error")
            
        except httpx.HTTPStatusError as e:
            st.error(f"❌ Backend error: {e.response.status_code}")
            st.caption(f"Response: {e.response.text}")
            status.update(label="❌ Request Failed", state="error")
            
        except Exception as e:
            st.error(f"❌ Unexpected error: {str(e)}")
            status.update(label="❌ Error", state="error")

# Footer
st.divider()
st.caption("🔐 Powered by Azure OpenAI + AKS + Databricks Delta Lake")