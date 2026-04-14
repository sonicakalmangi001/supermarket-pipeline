FROM python:3.11-slim

WORKDIR /app

COPY requirements/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/etl.py .

ENV PORT=8080

CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 300 etl:app
