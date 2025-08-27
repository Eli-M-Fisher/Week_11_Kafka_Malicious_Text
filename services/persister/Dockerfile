FROM python:3.13-slim

WORKDIR /app

# first i copy requirements and install
COPY ../../requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# and copy retriever service files
COPY . .

CMD ["python", "main.py"]