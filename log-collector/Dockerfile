FROM python:3.10-slim
 
# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev make \
&& rm -rf /var/lib/apt/lists/*
 
# Set working directory
WORKDIR /app
 
# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
 
# Copy application code
COPY app.py .
 
# Environment variables
ENV PYTHONUNBUFFERED=1
 
# Expose app port
EXPOSE 5002
 
# Run the app
CMD ["python", "app.py"]
