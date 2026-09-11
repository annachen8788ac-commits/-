FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py ./
COPY import_history.py ./
COPY history_parts ./history_parts
ENV PORT=8000
CMD ["sh","-c","python import_history.py && uvicorn app:app --host 0.0.0.0 --port ${PORT:-8000}"]
