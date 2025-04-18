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
    print(f"Attempting to create project with name: {name}")  # Debug log
    conn = get_db_connection()
    if conn is None:
        print("Failed to establish database connection")  # Debug log
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
        print(f"Successfully created project with ID: {project_id}")  # Debug log
        conn.commit()
        cur.close()
        conn.close()
        return project_id
    except Exception as e:
        print(f"Error creating project: {e}")  # Debug log
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
    
def get_datasets(project_id):
    """Fetches all datasets for a given project."""

    
    # Fetch all relevant dataset fields except target and created_at
    return fetch_data(
        "SELECT id, name, source_type, eol_definition, feature_lookup_definition, source_table,"
        " evaluation_type, percentage, eval_table_name, split_time_column, materialized,"
        " training_table_name, eval_table_name_generated"
        " FROM datasets WHERE project_id = %s ORDER BY name ASC;",
        params=(project_id,)
    )

def create_dataset(project_id, name, source_type,
                   eol_definition, feature_lookup_definition,
                   source_table, evaluation_type, percentage,
                   eval_table_name, split_time_column,
                   materialized, training_table_name,
                   eval_table_name_generated):
    """
    Inserts a new dataset for the given project and returns the new dataset ID.
    """
    conn = get_db_connection()
    if conn is None:
        return None
    try:
        cur = conn.cursor()
        # Insert new dataset and return its ID
        cur.execute(
            """
            INSERT INTO datasets (
                project_id, name, source_type, eol_definition,
                feature_lookup_definition, source_table, evaluation_type,
                percentage, eval_table_name, split_time_column,
                materialized, training_table_name, eval_table_name_generated
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id;
            """,
            (
                project_id, name, source_type, eol_definition,
                feature_lookup_definition, source_table, evaluation_type,
                percentage, eval_table_name, split_time_column,
                materialized, training_table_name, eval_table_name_generated
            )
        )
        dataset_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()
        return dataset_id
    except Exception as e:
        print(f"Error creating dataset: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        finally:
            conn.close()
        return None

def update_dataset(dataset_id, name, source_type,
                   eol_definition, feature_lookup_definition,
                   source_table, evaluation_type, percentage,
                   eval_table_name, split_time_column,
                   materialized, training_table_name,
                   eval_table_name_generated):
    """Updates an existing dataset and returns the dataset ID if successful."""
    conn = get_db_connection()
    if conn is None:
        return None
    try:
        cur = conn.cursor()
        # Update dataset record
        cur.execute(
            """
            UPDATE datasets SET
                name = %s,
                source_type = %s,
                eol_definition = %s,
                feature_lookup_definition = %s,
                source_table = %s,
                evaluation_type = %s,
                percentage = %s,
                eval_table_name = %s,
                split_time_column = %s,
                materialized = %s,
                training_table_name = %s,
                eval_table_name_generated = %s
            WHERE id = %s;
            """,
            (
                name, source_type, eol_definition,
                feature_lookup_definition, source_table,
                evaluation_type, percentage, eval_table_name,
                split_time_column, materialized,
                training_table_name, eval_table_name_generated,
                dataset_id
            )
        )
        conn.commit()
        cur.close()
        conn.close()
        return dataset_id
    except Exception as e:
        print(f"Error updating dataset: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        finally:
            conn.close()
        return None
