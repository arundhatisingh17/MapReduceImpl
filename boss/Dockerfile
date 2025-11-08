FROM python:3.10-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --upgrade protobuf==6.31.1 grpcio grpcio-tools

COPY . /app
EXPOSE 50052
CMD ["python", "server.py"]
