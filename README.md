### Python Script for Loading CSV Data from S3 Bucket to Redshift Table

This code is a Python script that loads data from a CSV file in an S3 bucket into a Redshift database table. It does the following:

1. Import the necessary Python libraries - `boto3`, `psycopg2`, and `logging`.
2. Set up AWS and Redshift credentials, S3 bucket and file path, and logging file name.
3. Create a connection to the Redshift database using `psycopg2`.
4. Create a connection to AWS Glue using `boto3`.
5. Retrieve information about the table being loaded from AWS Glue catalog using `get_table()` method.
6. Map data types from the table retrieved from AWS Glue to appropriate PostgreSQL data types.
7. Create a schema for the Redshift table by iterating over columns of the table retrieved from AWS Glue and mapping data types to appropriate PostgreSQL data types.
8. Check if the table exists in the Redshift database and drop it if it does.
9. Create a new table in the Redshift database using `CREATE TABLE` command with the schema defined in step 7.
10. Load data from the CSV file in the S3 bucket into the Redshift table using `COPY` command with the parameters provided.
11. Log successful or error messages during the execution.
12. Close connections to Redshift and Glue.

**Note:** You should replace the placeholders with your own AWS and Redshift credentials, S3 bucket and file path, database and table name.

