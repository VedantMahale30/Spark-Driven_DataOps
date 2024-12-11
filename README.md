# Spark-Driven_DataOps
Automated data pipeline for importing, transforming, and preparing data from MySQL to HDFS and BI tools using Sqoop, Spark, and crontab scheduling

Project Overview: End-to-End Data Pipeline for Business Intelligence

This project involves automating the entire data pipeline from extracting data from MySQL to transforming and storing it in a Data Warehouse for Business Intelligence (BI) purposes. 
The pipeline uses Sqoop, HDFS, Apache Spark, and SFTP to ensure smooth and efficient data processing.


Components and Workflow:

1. MySQL (Data Source)
- Purpose: Stores raw transactional data.
- Process:
  - Data can include tables such as Sales, Orders, Customers, and more.
  - The goal is to move this raw data to HDFS for further processing.

2. Sqoop (Data Extraction to HDFS)
- Purpose: Automates the process of importing all tables from MySQL to HDFS.
- Steps:
  - Use the `sqoop-credentials` option file to securely store MySQL credentials.
  - Create a Sqoop script (`sqoop-script.sh`) to handle the import of all tables from MySQL into HDFS (Data Lake).

3. HDFS (Data Lake)
- Purpose: Serves as the storage layer for raw data.
- Details:
  - Data imported from MySQL is stored here.
  - The data is organized in a partitioned or directory structure, such as by date or department.

4. Apache Spark (Data Transformation)
- Purpose: Transforms raw data into a more structured format suitable for reporting and analytics.
- Automation:
  - Use `spark-submit` in a crontab job to automatically run Spark transformation jobs at scheduled times (e.g., daily).
    
- Transformation Examples:
  - Clean the data (e.g., remove duplicates).
  - Aggregate data (e.g., sum sales by region).
  - Join multiple tables and save the result in HDFS for reporting.

5. HDFS (Data Warehouse)
- Purpose: Stores cleaned and transformed data for reporting and analytics.
- Details:
  - The transformed data is stored in an optimized format like Parquet or ORC for efficient querying and analysis.
  - This layer is designed to support BI tools and machine learning algorithms.

6. Sqoop or SFTP for BI Integration
- Purpose: Export data from HDFS to a BI tool or another database for reporting and analysis.
- Steps:
  - Use Sqoop or SFTP to export the transformed data from HDFS to a database or BI tool like Tableau, Power BI, or custom dashboards.

7. Crontab for Automation
- Purpose: Automate the entire data pipeline to run at scheduled times.
- Details:
  - Sqoop Job: Schedule the Sqoop import process to run at regular intervals.
  - Spark Job: Use crontab to schedule Spark transformation jobs every day at midnight, for example.
- Example Crontab for Spark Job:
  
Tools Involved:
- MySQL: Data source.
- Sqoop: For importing data from MySQL to HDFS.
- HDFS: Data storage (Data Lake and Data Warehouse).
- Apache Spark: Data transformation.
- Linux: Automate job scheduling.
- SFTP/Sqoop: Export data for Business Intelligence (BI) purposes.
