FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY csv_to_kafka.py .

CMD ["python", "/app/csv_to_kafka.py"]
