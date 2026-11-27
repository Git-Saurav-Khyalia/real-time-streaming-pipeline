# Spark Structured Streaming + Delta Lake image (also used for ad-hoc jobs).
FROM python:3.11-slim-bookworm

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps tini \
 && rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    SPARK_IVY_DIR=/opt/ivy

WORKDIR /app

COPY lakehouse/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY lakehouse ./lakehouse
COPY quality ./quality
COPY warehouse ./warehouse

# Resolve Delta / Kafka / hadoop-aws jars at build time so the container starts offline-fast.
RUN mkdir -p /opt/ivy && python -m lakehouse.prefetch

ENTRYPOINT ["tini", "--"]
CMD ["python", "-m", "lakehouse.run_streaming"]
