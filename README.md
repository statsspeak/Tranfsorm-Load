# Redshift data loading script

This Python script reads configuration values from a `config.ini` file and uses those values to load data from an S3 bucket into a Redshift table. The script uses the following libraries:

- `configparser` for reading the configuration file
- `boto3` for interacting with AWS services
- `psycopg2` for connecting to and interacting with a Redshift database
- `logging` for logging status messages to a file

## Configuration

The script reads the following values from the `config.ini` file:

- AWS access key ID and secret access key for authentication
- Redshift database endpoint, username, password, port, and database name
- S3 bucket name and file path for the input data file
- AWS Glue catalog database name and table name for the input data schema

## Data loading process

The script performs the following steps:

1. Reads configuration values from `config.ini`
2. Creates a connection to Redshift and AWS Glue using the configuration values
3. Retrieves schema information for the input data from the AWS Glue catalog
4. Creates a Redshift table with the same schema as the input data
5. Loads the input data from S3 into the Redshift table using the COPY command
6. Logs the success or failure of the data loading process

## Usage

1. Create a `config.ini` file with the required configuration values
2. Run the `redshift_load.py` script

Note: Before running the script, make sure that the Redshift cluster and S3 bucket are properly configured and accessible.
