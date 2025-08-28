FROM python:3.11-slim

WORKDIR /app

# עדכוני pip ותלויות
RUN pip install --no-cache-dir --upgrade pip

# התקנות חיוניות
RUN pip install --no-cache-dir fastapi uvicorn[standard] kafka-python scikit-learn

COPY app /app

EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
