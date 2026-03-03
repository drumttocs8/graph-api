FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY app/ .

# Environment
ENV PYTHONUNBUFFERED=1
ENV PORT=8083

EXPOSE 8083

# Use shell form to expand $PORT at runtime
CMD uvicorn main:app --host 0.0.0.0 --port ${PORT:-8083}
