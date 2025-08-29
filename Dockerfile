FROM python:3.12.10-slim-bookworm

WORKDIR /app

# Copy requirements files
COPY requirements.txt requirements-dev.txt ./

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir -r requirements-dev.txt \
    && pip install --no-cache-dir pytest-xdist pytest-html pytest-json-report

# Copy the application
COPY . .

# Create directory for binlog data with proper permissions
RUN mkdir -p /app/binlog && chmod 777 /app/binlog

# Make the main script executable
RUN chmod +x /app/main.py

# Set the entrypoint to the main script
ENTRYPOINT ["/app/main.py"]

# Default command (can be overridden in docker-compose)
CMD ["--help"]
