FROM python:3.11-slim

WORKDIR /app
RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir fastapi uvicorn[standard] kafka-python pymongo

COPY app /app
EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
