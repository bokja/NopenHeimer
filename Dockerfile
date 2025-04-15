# Use official Python image
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Copy everything in
COPY . .

# Install dependencies
RUN pip install --upgrade pip && pip install -r requirements.txt

# Default command (override in docker-compose)
CMD ["bash"]
