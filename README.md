# Lakehouse ETL Project

Enterprise-grade ETL solution for data lakehouse architecture using Apache Iceberg, Spark, and Airflow.

## Project Structure

- /src - Main application source code
- /tests - Unit and integration tests  
- /configs - Configuration files
- /sql - SQL templates and transformations
- /docker - Docker configuration
- /scripts - Deployment and utility scripts

## Setup

1. Install Python 3.11+
2. Install requirements: pip install -r requirements.txt
3. Configure environment in configs/environments/
4. Run tests: python -m pytest tests/

## Architecture

- **Raw Layer**: Original data from source systems
- **Normalised Layer**: 3NF normalized data with SCD Type 2
- **Semantic Layer**: Analytics-ready views and OLAP cubes

## Technologies

- Apache Spark (PySpark)
- Apache Iceberg
- Apache Airflow
- MinIO (S3-compatible storage)
- Dremio (SQL engine)