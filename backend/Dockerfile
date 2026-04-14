FROM python:3.11-slim

WORKDIR /app

COPY backend/requirements.txt .
RUN pip install --upgrade pip && pip install --no-cache-dir --timeout=120 -r requirements.txt

COPY backend/ ./backend/
COPY scripts/ ./scripts/

EXPOSE 8000

CMD ["uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "8000"]
