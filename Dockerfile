FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir flask

COPY app.py /app/
COPY data/ /data/

EXPOSE 5000

CMD ["python", "app.py"]
