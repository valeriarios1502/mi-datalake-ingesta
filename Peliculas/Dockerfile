FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY contenedor2.py .
RUN mkdir -p /app/output
CMD ["python", "contenedor2.py"]