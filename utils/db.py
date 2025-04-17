import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv

# Load database credentials from .env file
load_dotenv()

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

def get_db_connection():
    """Establishes a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

def fetch_data(query, params=None):
    """Fetches data from the database using the provided query."""
    conn = get_db_connection()
    if conn:
        try:
            df = pd.read_sql_query(query, conn, params=params)
            conn.close()
            return df
        except Exception as e:
            print(f"Error fetching data: {e}")
            conn.close()
    return pd.DataFrame()

def get_projects():
    """Fetches all projects from the database."""
    return fetch_data(
        "SELECT id, name, description, catalog, schema FROM projects ORDER BY name ASC;"
    )

def create_project(name, description, catalog, schema):
    """
    Inserts a new project into the database and returns the new project ID.
    """
    conn = get_db_connection()
    if conn is None:
        return None
    try:
        cur = conn.cursor()
        # Insert new project and return its ID
        cur.execute(
            """
            INSERT INTO projects (name, description, catalog, schema)
            VALUES (%s, %s, %s, %s)
            RETURNING id;
            """,
            (name, description, catalog, schema)
        )
        project_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()
        return project_id
    except Exception as e:
        print(f"Error creating project: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        finally:
            conn.close()
        return None

def update_project(project_id, name, description, catalog, schema):
    """
    Updates an existing project in the database and returns the project ID if successful.
    """
    conn = get_db_connection()
    if conn is None:
        return None
    try:
        cur = conn.cursor()
        # Update project record
        cur.execute(
            """
            UPDATE projects
            SET name = %s,
                description = %s,
                catalog = %s,
                schema = %s
            WHERE id = %s;
            """,
            (name, description, catalog, schema, project_id)
        )
        conn.commit()
        cur.close()
        conn.close()
        return project_id
    except Exception as e:
        print(f"Error updating project: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        finally:
            conn.close()
        return None
