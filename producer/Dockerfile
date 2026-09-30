FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY generate_sample_data.py .
COPY producer.py .
COPY sample_data/ ./sample_data/

RUN mkdir -p /app/logs
ENV LOG_DIR=/app/logs

# Sample data is baked in at build time (generated once, then shipped in the
# image). Re-run generate_sample_data.py and rebuild if you want fresh data.

CMD ["python", "producer.py"]
