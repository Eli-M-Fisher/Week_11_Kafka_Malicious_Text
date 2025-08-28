FROM python:3.13-slim

WORKDIR /app

# copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# download required NLTK resources
RUN python -m nltk.downloader stopwords punkt wordnet

# copy only this service's code
COPY services/preprocessor/ .

CMD ["python", "main.py"]