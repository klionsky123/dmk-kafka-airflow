# Kafka python pipelines with Apache Airflow

---

## About

POC for the ingestion of the streaming data into the database with Kafka and Apache Airflow.

### Watch the Introduction Video on YouTube (5 min):


## ✅ Main Features

- Contains a fully functional, stand-alone set of metadata tables, stored procedures, and python modules to support ETL processes.
- Extracts sample data from various data sources—flat files, relational databases (RDBMS), REST APIs(token-based auth, pagination, etc.), Kafka streaming, —with a strong emphasis on performance.
- Scheduled and orchestrated through Apache Airflow.

---

## Tech Stack

- **Kafka**
- **Apache Airflow**
- **Python**
- **MS SQL Server**
- **Docker** 

---

## Project Notes

- MSSQL serves as the destination server and contains both 'Stage' and 'Production' databases
- Apache Airflow, running in Docker, is used for the job scheduling and ETL orchestration.
  
### Project Architecture: 

<img src="diagrams/Kafka-Project-architecture.jpg" alt="Example" width="500" hight="300"/>

---

### ETL-Metadata-tables schema 

<img src="diagrams/metadata-db-schema.jpg" alt="Example" width="500" hight="300"/>

### Documentation:

[AirFlow-ETL-Presentation](https://github.com/klionsky123/dmk-airflow-etl/blob/main/diagrams/AirFlow-ETL-Presentation.pdf)

---

## Road Map

- Add Use cases for streaming data (Kafka), Parque files (AWS)
- Add support for PostgreSQL metadata store (currently, MS SQL Server only)



