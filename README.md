## Description
---
* This repo contains projects done in data engineering. 

## Projects
---
1. <ins> Postgres ETL </ins> :heavy_check_mark:
* This project looks at data modelling for a fictitious music startup Sparkify, applying STAR schema to ingest data to simplify queries that answers business questions the product owner may have

2. <ins> Cassandra ETL </ins> :heavy_check_mark:
* Looking at the realm of big data, Cassandra helps to ingest large amounts of data in a NoSQL context. This project adopts a query centric approach in ingesting data into data tables in Cassandra, to answer business questions about a music app

3. <ins> Data Warehousing with AWS Redshift </ins> :heavy_check_mark:
* This project creates a data warehouse, in AWS Redshift. A data warehouse provides a reliable and consistent foundation for users to query and answer some business questions based on requirements.

4. <ins> Data Lake with Spark & AWS S3 </ins> :heavy_check_mark:
* This project creates a data lake, in AWS S3 using Spark. 
* Why create a data lake? A data lake provides a reliable store for large amounts of data, from unstructured to semi-structured and even structured data. In this project, we ingest json files, denormalize them into fact and dimension tables and upload them into a AWS S3 data lake, in the form of parquet files.

5. <ins> Data Pipelining with Airflow </ins> :heavy_check_mark:
* This project schedules data pipelines, to perform ETL from json files in S3 to Redshift using Airflow. 
* Why use Airflow? Airflow allows workflows to be defined as code, they become more maintainable, versionable, testable, and collaborative
