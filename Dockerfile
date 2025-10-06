# Start with a standard Python 3.10 image
FROM python:3.10-slim

# Set the working directory inside the container
WORKDIR /app

# IMPORTANT: Update the package manager and install ffmpeg
RUN apt-get update && apt-get install -y ffmpeg

# Copy the file that lists our Python libraries
COPY requirements.txt .

# Install the Python libraries
RUN pip install --no-cache-dir -r requirements.txt

# Copy all of your other code (the .py and .ttf files) into the container
COPY . .

# Set the command to run when the container starts
CMD ["python", "televideditor.py"]
