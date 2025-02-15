FROM python:3.12-slim-bookworm

# Set the working directory in the container
WORKDIR /app

# Copy requirements.txt and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application files
COPY . .

# Ensure environment variables are loaded
ENV PYTHONUNBUFFERED=1

# Command to run the application with uv
CMD ["python", "api.py"]
