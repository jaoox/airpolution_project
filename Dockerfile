FROM apache/airflow:2.7.1

USER root

# System dependencies including OpenJDK for Spark
RUN apt-get update && \
    apt-get install -y \
    openjdk-11-jdk \
    gdal-bin \
    libgdal-dev \
    python3-gdal \
    postgresql-client && \
    rm -rf /var/lib/apt/lists/*

# Set Java environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

USER airflow

# Install Python packages including python-dotenv
RUN pip install --no-cache-dir \
    pyspark==3.4.1 \
    geopandas==0.13.2 \
    rasterio==1.3.8 \
    minio==7.1.15 \
    psycopg2-binary==2.9.7 \
    apache-airflow-providers-amazon==8.10.0 \
    apache-airflow-providers-postgres==5.5.1 \
    boto3==1.28.10 \
    awscli==1.29.10 \
    python-dotenv==1.0.0 \
    --upgrade pip