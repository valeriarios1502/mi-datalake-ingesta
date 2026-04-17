FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY contenedor3.py .
RUN mkdir -p /app/output
CMD ["python", "contenedor3.py"]