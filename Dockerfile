FROM python:3.11-slim

WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY src/ ./src/
COPY static/ ./static/
COPY templates/ ./templates/
COPY run_web.py .

# Index is downloaded from Azure Blob Storage at runtime
# Create directories for index and temp files
RUN mkdir -p /tmp/flask-session /app/index

# Verify the app can be imported
RUN python -c "from src.web import create_app; app = create_app(); print('App created successfully')"

EXPOSE 5000

# Use production WSGI server
CMD ["gunicorn", "--bind", "0.0.0.0:5000", "--workers", "1", "--threads", "4", "--timeout", "120", "--log-level", "info", "run_web:app"]
