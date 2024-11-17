import psycopg2
import json
import boto3
import os
import zipfile
from io import BytesIO

bucket_name = 'saas-transformed-data'

def get_db_connection():
    print('starting connection')
    conn = psycopg2.connect(
        host="mydb.cfewq6omsbvz.us-east-1.rds.amazonaws.com",
        database="mydb",
        user="saas",
        password="SAAS2024",
        port="5432"
    )
    print('connection made')
    return conn

def storeMetadataOnRDS(metadata):

    conn = None
    try:
        conn = get_db_connection()

        with conn.cursor() as cur:
        
            # Check if the table exists before dropping
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'saas_metadata'
                );
            """)
            table_exists = cur.fetchone()[0]

            if table_exists:
                print('Table exists. Deletig all rows...')
                cur.execute("DELETE FROM SAAS_METADATA")
                print('Table emptied.')
            else:
                # Create a new table structure based on the metadata JSON
                create_table_query = """
                    CREATE TABLE SAAS_METADATA (
                        ATTRIBUTE VARCHAR(255),
                        VALUE JSONB
                    );
                """
                cur.execute(create_table_query)
                print('New table created.')

            # Insert metadata into the table
            for key, value in metadata.items():
                cur.execute(
                    "INSERT INTO SAAS_METADATA (ATTRIBUTE, VALUE) VALUES (%s, %s)",
                    (key, json.dumps(value))
                )
            print('Metadata inserted.') 

            cur.execute("SELECT * FROM SAAS_METADATA;")
            metadataTable = cur.fetchall()

        conn.commit()

    except(Exception) as error:
        print(f'Error: {error}')
        conn.rollback()
    finally:
        if conn != None:
            conn.close()
            print('database connection closed')
        else:
            print('connection not made')

    return metadataTable


def downloadFilesFromS3():
    # Initialize the S3 client
    s3 = boto3.client('s3')
    
    # Initialize the zip file in memory
    zip_buffer = BytesIO()
    
    # Create a zip file in memory
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        # List objects in the S3 bucket
        paginator = s3.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=bucket_name)

        # Loop through the pages of the S3 bucket and download the files
        for page in page_iterator:
            if 'Contents' in page:
                for obj in page['Contents']:
                    file_name = obj['Key']
                    print(f"Downloading {file_name}...")

                    # Download each file into memory (instead of to disk)
                    file_obj = s3.get_object(Bucket=bucket_name, Key=file_name)
                    file_data = file_obj['Body'].read()

                    # Add file to the zip archive
                    zip_file.writestr(file_name, file_data)

    # Return the zip file in memory
    zip_buffer.seek(0)  # Go to the beginning of the BytesIO buffer
    return zip_buffer


def putFilesToS3(fileNames):

    s3 = boto3.Session().client('s3')

    for fileName in fileNames:
        s3.upload_file('/tmp/'+fileName, bucket_name, fileName)