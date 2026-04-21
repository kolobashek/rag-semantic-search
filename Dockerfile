FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    HF_HOME=/data/cache/huggingface \
    SENTENCE_TRANSFORMERS_HOME=/data/cache/sentence-transformers

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        curl \
        libglib2.0-0 \
        libgl1 \
        poppler-utils \
        tesseract-ocr \
        tesseract-ocr-eng \
        tesseract-ocr-rus \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md ./
COPY src ./src
COPY assets ./assets
COPY config.docker.example.json ./config.docker.example.json
COPY nice_app.py telegram_bot.py index_rag.py rag_search.py rag_search_fixed.py ocr_pdfs.py rag_core.py telemetry_db.py user_auth_db.py ./

RUN pip install --upgrade pip \
    && pip install ".[ocr]"

RUN cp config.docker.example.json config.json \
    && mkdir -p /data/catalog /data/state /data/logs /data/cache

EXPOSE 8080

CMD ["rag-web", "--host", "0.0.0.0", "--port", "8080", "--no-show"]
