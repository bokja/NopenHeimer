FROM python:3.12-slim

WORKDIR /app

# Create a non-root user and group
RUN groupadd -r appuser && useradd -r -g appuser appuser

COPY . .

# Change ownership of the app directory
RUN chown -R appuser:appuser /app

# Add /app to PYTHONPATH
ENV PYTHONPATH="${PYTHONPATH}:/app"

RUN pip install --upgrade pip && pip install -r requirements.txt

# Switch to the non-root user
USER appuser

CMD ["bash"]
