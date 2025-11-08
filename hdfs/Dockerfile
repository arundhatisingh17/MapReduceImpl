FROM python:3.10-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . /app
# generated pb2 files must be present; if not, run protoc in build step externally
EXPOSE 50051
CMD ["python", "server.py"]