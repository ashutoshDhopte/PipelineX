import psycopg2
import json

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
