FROM python:3.12-slim

WORKDIR /app
COPY . .

# Add /app to PYTHONPATH
ENV PYTHONPATH="${PYTHONPATH}:/app"

RUN pip install --upgrade pip && pip install -r requirements.txt

CMD ["bash"]
