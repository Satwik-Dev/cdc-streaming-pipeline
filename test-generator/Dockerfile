FROM python:3.9-slim

WORKDIR /app

# Install PostgreSQL client
RUN apt-get update && \
    apt-get install -y postgresql-client && \
    rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy test generator script
COPY generate_test_data.py .

# Set up entrypoint
ENTRYPOINT ["python", "generate_test_data.py"]