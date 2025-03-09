# Use an official Python runtime as a parent image
FROM python:3.9.6-slim AS build

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Ensure the script is executable
RUN chmod +x /app/run_ao_leaderboard.py

RUN python3 -m pip install --no-cache-dir -r requirements.txt

RUN mkdir /app/plots

ENV cloud_run=true

ENTRYPOINT ["python3"]

CMD ["/app/run_ao_leaderboard.py"]