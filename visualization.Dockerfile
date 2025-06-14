FROM python:3.9-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install dependencies in the correct order to avoid binary incompatibility
COPY visualization/requirements.txt .
RUN pip install --no-cache-dir numpy==1.23.5
RUN pip install --no-cache-dir pandas==1.5.3
RUN pip install --no-cache-dir -r requirements.txt

# Copy visualization files
COPY visualization /app/

# Expose Streamlit port
EXPOSE 8501

# Set environment variables
ENV MONGODB_HOST=mongodb
ENV MONGODB_PORT=27017
ENV MONGODB_DATABASE=market_data

# Run the dashboard
CMD ["streamlit", "run", "dashboard.py", "--server.port=8501", "--server.address=0.0.0.0"]