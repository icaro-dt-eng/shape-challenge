FROM python:3.8

# Install Java (required for PySpark)
RUN apt-get update && \
    apt-get install -y default-jre && \
    apt-get clean

# Install PySpark
RUN pip install pyspark

# Set the working directory in the container
WORKDIR /app

# Copy scripts into the container
COPY . .

# Install the Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Make the entrypoint script executable
RUN chmod +x entrypoint.sh

# Set the entrypoint command
ENTRYPOINT ["./entrypoint.sh"]
