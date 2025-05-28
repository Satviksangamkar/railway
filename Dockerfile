# railway/Dockerfile
FROM python:3.11-slim

WORKDIR /app

# install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# copy your tracker script
COPY tracker.py .

# tell Compose to run this by default
CMD ["python", "tracker.py"]
