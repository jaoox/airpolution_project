# Environmental Monitoring Data Pipeline

## Overview

This project is designed to ingest, process, and analyze environmental monitoring data from the OpenAQ API. The pipeline is built using Apache Airflow for orchestration, Apache Spark for data processing, and MinIO and PostgreSQL for storage. The data is transformed through multiple stages (Bronze, Silver, Gold) to ensure it is clean, enriched, and ready for analysis.

## Architecture

### Components

1. **Data Ingestion**:
   - **Source**: OpenAQ API
   - **Ingestion Script**: `apiaq_ingestion.py`
   - **Storage**: MinIO (S3-compatible) and AWS S3

2. **Data Processing**:
   - **Bronze to Silver Transformation**: `bronze_to_silver.py`
   - **Silver to Gold Transformation**: `silver_to_gold_firstAnalysis.py` and `silver_to_gold_secondAnalysis.py`
   - **Processing Engine**: Apache Spark

3. **Data Storage**:
   - **MinIO**: For raw and intermediate data storage
   - **PostgreSQL/PostGIS**: For final analyzed data storage

4. **Orchestration**:
   - **Apache Airflow**: For workflow orchestration and scheduling

5. **API Gateway**:
   - **FastAPI**: For exposing endpoints to interact with the data

### Workflow

1. **Ingestion**:
   - Data is fetched from the OpenAQ API and stored in MinIO and AWS S3 in the Bronze layer.

2. **Validation**:
   - The raw data is validated using Airflow's S3ListOperator.

3. **Transformation**:
   - **Bronze to Silver**: Data is cleaned and transformed into a structured format.
   - **Silver to Gold**: Data is further enriched and analyzed to produce final datasets.

4. **Loading**:
   - The final datasets are loaded into PostgreSQL/PostGIS for further analysis and querying.

## Technical Choices

- **Apache Airflow**: Chosen for its robust scheduling and workflow management capabilities.
- **Apache Spark**: Selected for its powerful data processing and transformation capabilities.
- **MinIO**: Used for its S3-compatibility and ease of setup.
- **PostgreSQL/PostGIS**: Chosen for its support for spatial data and robust querying capabilities.
- **FastAPI**: Selected for its performance and ease of use in building APIs.

## Installation and Build

### Prerequisites

- Docker
- Docker Compose
- Python 3.9

### Steps

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/your-repo/environmental-monitoring.git
   cd environmental-monitoring
