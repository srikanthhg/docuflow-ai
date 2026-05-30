FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
EXPOSE 8000

# Run Uvicorn with production settings
# --workers 2 is optimal for I/O bound DB queries
# --host 0.0.0.0 ensures container networking works
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]