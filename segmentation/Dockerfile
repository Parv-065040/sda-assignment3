FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY rfm_segmentation.py .

RUN mkdir -p /app/logs
ENV LOG_DIR=/app/logs

CMD ["python", "rfm_segmentation.py"]
