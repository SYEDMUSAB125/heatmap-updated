import logging
import psycopg2
import psycopg2.extras
from urllib.parse import urlparse
def get_db_connection():
    try:
        # Parse the connection string
        result = urlparse("postgresql://neondb_owner:HcG6QJDjWu3m@ep-cool-dream-a5y5ch7l-pooler.us-east-2.aws.neon.tech/neondb?sslmode=require")
        # Establish a connection using the parsed components
        conn = psycopg2.connect(
            dbname=result.path[1:],  # result.path will have a leading '/' which we need to strip
            user=result.username,
            password=result.password,
            host=result.hostname,
            port=result.port,
            sslmode="require"  # Ensure SSL is used as per your connection string
        )
        return conn
    except psycopg2.Error as e:
        logging.error(f"Database connection error: {e}")
        raise
get_db_connection()