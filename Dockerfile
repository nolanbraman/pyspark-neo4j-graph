# Base image with Python and Spark
FROM python:3.9-slim-buster

# Set the working directory in the container
WORKDIR /app

RUN yum -y install wget

# Install Java
RUN apt-get update && \
    apt-get install -y default-jdk && \
    apt-get clean

# Download and install Apache Spark
RUN wget -O spark-3.2.1-bin-hadoop3.2.tgz https://downloads.apache.org/spark/spark-3.2.1/spark-3.2.1-bin-hadoop3.2.tgz && \
    tar -xvzf spark-3.2.1-bin-hadoop3.2.tgz && \
    mv spark-3.2.1-bin-hadoop3.2 /spark && \
    rm spark-3.2.1-bin-hadoop3.2.tgz

# Set Spark environment variables
ENV SPARK_HOME=/spark
ENV PATH=$PATH:$SPARK_HOME/bin
ENV PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH

# Install PySpark
RUN pip install pyspark

# Copy the Python project files to the working directory
COPY . /app

# Set the entrypoint command
CMD ["python", "main.py"]
