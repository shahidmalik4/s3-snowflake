# S3 to Snowflake ETL Pipeline

This project implements an Extract, Transform, Load (ETL) pipeline to load Airbnb NYC data from an S3 bucket into a Snowflake database. The pipeline extracts CSV data from an S3 bucket, transforms it using Pandas, and loads it into Snowflake using the Snowflake Python connector.

## Prerequisites

Before running the project, ensure you have the following installed:

- Python 3.x
- `boto3` library for AWS S3 interaction
- `pandas` library for data manipulation
- `snowflake-connector-python` for connecting to Snowflake
- `python-dotenv` for managing environment variables
- Snowflake account and required permissions to create tables and load data
- AWS account with access to S3

## S3 Bucket Info
- **Bucket Name:** `s3-snowflake-etl-pipeline`
- **Data File Path:** `data/AIRBNB_NYC.csv`
- Ensure the CSV file is uploaded to the specified path in the S3 bucket.

## Running the Project

#### Cloning the Repo
To clone this repository, run the following command:
```bash
git clone https://github.com/shahidmalik4/s3-snowflake.git
&&
cd repo-directory
```

#### Install the required packages
```bash
pip install -r requirements.txt
```

#### Run the ETL script
```bash
python main.py
```
