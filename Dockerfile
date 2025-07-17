FROM python:3.10-slim

WORKDIR /app
COPY nordic_to_bok.py .
COPY requirements.txt .
COPY xslt ./xslt

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 8000
CMD ["uvicorn", "nordic_to_bok:app", "--host", "0.0.0.0", "--port", "3900"]