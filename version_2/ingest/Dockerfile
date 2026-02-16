FROM python:3.14-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY tfl_to_kafka.py .

CMD ["python", "tfl_to_kafka.py"]