FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY bank/ ./bank/
COPY scripts/ ./scripts/
COPY keys/ ./keys/

ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "bank.node"]
