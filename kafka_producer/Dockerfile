# Use a base image
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Copy application files
COPY . /app

# Install dependencies
RUN pip install -r requirements.txt

# Define environment variable
# ENV KAFKA_TOPIC=default_topic

# Default command (can be overridden)
CMD ["python", "app.py"]