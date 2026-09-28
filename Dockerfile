FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app/src

WORKDIR /app
COPY src /app/src
COPY scripts /app/scripts

EXPOSE 7000
ENTRYPOINT ["python", "-m", "dht_lab.cli"]
