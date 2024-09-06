# Use an official Python runtime as a parent image
FROM python:3.9.6-slim AS build

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Ensure the script is executable
RUN chmod +x /app/main.py

RUN python3 -m pip install --no-cache-dir -r requirements.txt

# Run the script using the Python interpreter
CMD ["python3", "/app/main.py"]
