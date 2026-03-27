FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip setuptools wheel && \
    pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONUNBUFFERED=1

# 使用 shell 形式，让 $PORT 被正确解析
CMD gunicorn app:app --bind 0.0.0.0:$PORT --workers 2 --timeout 600 --keep-alive 5
