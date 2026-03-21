FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    darktable \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .

ENV WORKER_NAME=render-worker
ENV RENDER_COMMAND=darktable-cli

EXPOSE 8080

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080"]