FROM python:3.13-slim

WORKDIR /app

# copy shared requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# copy only this service's code
COPY services/preprocessor/ .

CMD ["python", "main.py"]