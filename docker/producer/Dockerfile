FROM python:3.11-slim-bookworm

ENV PYTHONUNBUFFERED=1
WORKDIR /app

COPY data_generator/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY data_generator ./data_generator

ENTRYPOINT ["python", "data_generator/producer.py"]
CMD ["--bootstrap-servers", "kafka:29092", "--rate", "10"]
