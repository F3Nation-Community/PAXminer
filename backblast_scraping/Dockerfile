FROM python:3.11-slim-buster

WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application files
COPY . .

# Set the entrypoint for the container
CMD ["python", "PAXMiner_Cloud_Run.py", "A-Z"]