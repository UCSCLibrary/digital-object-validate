FROM python:3.11-slim

# Optimize python execution
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Copy source files
COPY bagit_profile.py .
COPY entry_point.py .
COPY requirements.txt .
COPY ucscbagit-v0.3.json .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Set up non-root user
RUN useradd -u 8888 worker && \
    chown -R worker:worker /app
USER worker

CMD ["python3", "entry_point.py"]
