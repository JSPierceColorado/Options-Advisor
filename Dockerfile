FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
HEALTHCHECK CMD python - <<'PY'\nimport sys; sys.exit(0)\nPY

CMD ["python", "main.py"]
