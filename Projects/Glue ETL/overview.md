Automated Serverless Data Lake ⚙️

Data engineering teams often spend significant time building and maintaining ETL pipelines. This project demonstrates how to automate the end-to-end data ingestion, cataloging, and transformation process using a fully serverless architecture, reducing operational overhead and improving time-to-value.

The solution leverages AWS services to build a scalable data pipeline that automatically detects, processes, and transforms incoming data into analytics-ready formats.

What you will learn:
Python for data processing and automation
Using Amazon S3 events to trigger AWS Lambda functions
Implementing AWS Glue Crawlers for automated schema discovery and data cataloging
Creating CloudWatch/EventBridge rules to trigger workflows based on events
Developing ETL pipelines using PySpark in AWS Glue
Storing transformed data in Amazon S3 for analytics consumption
Configuring Amazon SNS for notifications on Glue job success and failure
How the Glue Crawler helps:

A Glue Crawler automatically detects schema changes in incoming data. For example, if the file format changes from comma-delimited to pipe-delimited, the crawler adapts and updates the Glue Data Catalog accordingly, preventing pipeline failures due to schema inconsistencies.

Analytics Use Cases:
Sales analysis by outlet location type and outlet size (including counts and totals)
Sales analysis by outlet location type, outlet size, and item type
Sales aggregation by item type
Architecture 1: Purely on AWS
