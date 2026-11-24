FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DATA_DIR=/app/data

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY archiver_bot.py .

RUN useradd --create-home --shell /bin/bash appuser \
    && mkdir -p /app/data/media_cache \
    && chown -R appuser:appuser /app

USER appuser

EXPOSE 7860

CMD ["python", "archiver_bot.py"]
