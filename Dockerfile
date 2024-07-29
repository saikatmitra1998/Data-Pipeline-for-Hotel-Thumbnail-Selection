# Use an official Python runtime as a parent image
FROM openjdk:8-jdk-slim

# Install dependencies
RUN apt-get update && apt-get install -y python3 python3-pip wget

# Install Spark
RUN wget https://archive.apache.org/dist/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz \
    && tar -xvf spark-3.3.2-bin-hadoop3.tgz -C /opt/ \
    && rm spark-3.3.2-bin-hadoop3.tgz

ENV SPARK_HOME=/opt/spark-3.3.2-bin-hadoop3
ENV PATH=$SPARK_HOME/bin:$PATH

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip3 install --no-cache-dir -r requirements.txt

# Make port 8080 available to the world outside this container
EXPOSE 8080

# Define environment variable
ENV NAME=World

# Run main.py when the container launches
CMD ["python3", "main.py", "--images", "data/images.jsonl", "--tags", "data/image_tags.jsonl", "--main_images", "data/main_images.jsonl", "--output_cdc", "output/output_cdc.jsonl", "--output_snapshot", "output/output_snapshot.jsonl", "--output_metrics", "output/output_metrics.jsonl"]