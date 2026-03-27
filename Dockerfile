FROM python:3.11-slim

WORKDIR /app

# 复制 requirements.txt 并安装依赖
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip setuptools wheel && \
    pip install --no-cache-dir -r requirements.txt

# 复制所有项目文件
COPY . .

# 设置环境变量（可选）
ENV PYTHONUNBUFFERED=1

# 启动命令
CMD ["gunicorn", "app:app", "--bind", "0.0.0.0:${PORT:-5000}", "--workers", "2", "--timeout", "600", "--keep-alive", "5"]