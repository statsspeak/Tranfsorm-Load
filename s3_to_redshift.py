import boto3
import psycopg2
import logging

# Define AWS credentials
aws_access_key_id = 'YOUR_ACCESS_KEY_ID'
aws_secret_access_key = 'YOUR_SECRET_ACCESS_KEY'

# Define Redshift credentials
redshift_endpoint = 'YOUR_REDSHIFT_ENDPOINT'
redshift_user = 'YOUR_REDSHIFT_USER'
redshift_password = 'YOUR_REDSHIFT_PASSWORD'
redshift_port = '5439'
redshift_database = 'YOUR_REDSHIFT_DATABASE'

# Define S3 bucket and file path
s3_bucket = 'YOUR_S3_BUCKET'
s3_file_path = 'YOUR_S3_FILE_PATH'

# Set up logging
logging.basicConfig(filename='redshift_load.log', level=logging.INFO)

try:
    # Create Redshift connection
    redshift_conn = psycopg2.connect(
        host=redshift_endpoint,
        port=redshift_port,
        user=redshift_user,
        password=redshift_password,
        database=redshift_database
    )

    # Create AWS Glue connection
    glue_conn = boto3.client(
        'glue',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name='us-east-1'
    )

    # Get table information from AWS Glue catalog
    table_info = glue_conn.get_table(
        DatabaseName='YOUR_DATABASE_NAME',
        Name='YOUR_TABLE_NAME'
    )

    # Create Redshift table schema
    redshift_schema = ''
    for column in table_info['Table']['StorageDescriptor']['Columns']:
        col_name = column['Name']
        data_type = column['Type'].lower()
        # Map data types to appropriate PostgreSQL data types
        if data_type == 'string':
            data_type = 'varchar(max)'  # Replace with appropriate length
        elif data_type == 'double':
            data_type = 'double precision'
        elif data_type == 'long':
            data_type = 'bigint'
        redshift_schema += f"{col_name} {data_type}, "
    redshift_schema = redshift_schema[:-2]

    # Define Redshift table name
    redshift_table = 'YOUR_REDSHIFT_TABLE_NAME'

    # Check if the table exists and drop it if it does
    with redshift_conn.cursor() as cursor:
        cursor.execute(f"DROP TABLE IF EXISTS {redshift_table}")
        redshift_conn.commit()

    # Create Redshift table
    with redshift_conn.cursor() as cursor:
        cursor.execute(
            f'CREATE TABLE {redshift_table} ({redshift_schema})'
        )
        redshift_conn.commit()

    # Load data from S3 into Redshift table
    with redshift_conn.cursor() as cursor:
        cursor.execute(
            f"""
                COPY {redshift_table}
                FROM 's3://{s3_bucket}/{s3_file_path}'
                ACCESS_KEY_ID '{aws_access_key_id}'
                SECRET_ACCESS_KEY '{aws_secret_access_key}'
                FORMAT AS CSV
                IGNOREHEADER 1
                MAXERROR AS 1
            """
        )
        redshift_conn.commit()

    logging.info("Data load successful!")
except Exception as e:
    logging.error(str(e))
finally:
    # Close connections
    redshift_conn.close()
    glue_conn.close()
