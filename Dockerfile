FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY src/app.py .
COPY src/data_management.py .
COPY src/templates/ ./templates/
COPY src/static/ ./static/
COPY assets/ ./assets/

# Expose port
EXPOSE 5000

# Run application
CMD ["python", "app.py"]
