import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor

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

    
    # Fetch all relevant dataset fields including target
    return fetch_data(
        "SELECT id, name, source_type, eol_definition, feature_lookup_definition, source_table,"
        " timestamp_col, evaluation_type, percentage, eval_table_name, split_time_column, materialized,"
        " training_table_name, eval_table_name_generated, target"
        " FROM datasets WHERE project_id = %s ORDER BY name ASC;",
        params=(project_id,)
    )

def create_dataset(project_id, name, source_type,
                   eol_definition, feature_lookup_definition,
                   source_table, evaluation_type, percentage,
                   eval_table_name, split_time_column, timestamp_col,
                   materialized, training_table_name,
                   eval_table_name_generated, target):
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
                percentage, eval_table_name, split_time_column, timestamp_col,
                materialized, training_table_name, eval_table_name_generated, target
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id;
            """,
            (
                project_id, name, source_type, eol_definition,
                feature_lookup_definition, source_table, evaluation_type,
                percentage, eval_table_name, split_time_column,
                timestamp_col,
                materialized, training_table_name, eval_table_name_generated,
                target
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
                   eval_table_name, split_time_column, timestamp_col,
                   materialized, training_table_name,
                   eval_table_name_generated, target):
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
                timestamp_col = %s,
                materialized = %s,
                training_table_name = %s,
                eval_table_name_generated = %s,
                target = %s
            WHERE id = %s;
            """,
            (
                name, source_type, eol_definition,
                feature_lookup_definition, source_table,
                evaluation_type, percentage, eval_table_name,
                split_time_column, timestamp_col,
                materialized, training_table_name, eval_table_name_generated,
                target,
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

def delete_dataset(dataset_id):
    """Deletes a specific dataset from the database.
    Returns True if successful, False otherwise.
    """
    conn = get_db_connection()
    if conn is None:
        return False
    try:
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM datasets WHERE id = %s;",
            (dataset_id,)
        )
        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception as e:
        print(f"Error deleting dataset {dataset_id}: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        finally:
            conn.close()
        return False

def delete_project(project_id):
    """Deletes a project and its associated datasets from the database."""
    conn = get_db_connection()
    if conn is None:
        return False
    try:
        cur = conn.cursor()
        # First, delete associated datasets (or handle FK constraints appropriately)
        # Assuming ON DELETE CASCADE is set for datasets.project_id
        # If not, you'd delete datasets first:
        # cur.execute("DELETE FROM datasets WHERE project_id = %s;", (project_id,))

        # Delete the project
        cur.execute("DELETE FROM projects WHERE id = %s;", (project_id,))
        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception as e:
        print(f"Error deleting project {project_id}: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        finally:
            conn.close()
        return False

# --- Training Job Functions ---

def get_training_job(project_id, dataset_id):
    """Fetches a training job record by project and dataset ID."""
    query = "SELECT id, job_id, parameters FROM training WHERE project_id = %s AND dataset_id = %s;"
    df = fetch_data(query, params=(project_id, dataset_id))
    if not df.empty:
        return df.iloc[0].to_dict() # Return the first row as a dictionary
    return None

def create_training_job_record(project_id, dataset_id, parameters):
    """Inserts a new training job record and returns its ID."""
    conn = get_db_connection()
    if conn is None:
        return None
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO training (project_id, dataset_id, parameters)
            VALUES (%s, %s, %s::jsonb)
            RETURNING id;
            """,
            (project_id, dataset_id, parameters) # Assuming parameters is a JSON string
        )
        training_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()
        return training_id
    except Exception as e:
        print(f"Error creating training job record: {e}")
        try: conn.rollback()
        except Exception: pass
        finally: conn.close()
        return None

def update_training_job_id(training_id, job_id):
    """Updates the job_id for a specific training record."""
    conn = get_db_connection()
    if conn is None:
        return False
    try:
        cur = conn.cursor()
        cur.execute(
            "UPDATE training SET job_id = %s, updated_at = CURRENT_TIMESTAMP WHERE id = %s;",
            (job_id, training_id)
        )
        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception as e:
        print(f"Error updating training job_id for ID {training_id}: {e}")
        try: conn.rollback()
        except Exception: pass
        finally: conn.close()
        return False

def get_dataset_details(dataset_id):
    """ Fetches specific details for a given dataset ID. """
    query = "SELECT name, target, training_table_name FROM datasets WHERE id = %s;"
    df = fetch_data(query, params=(dataset_id,))
    if not df.empty:
        return df.iloc[0].to_dict()
    return None

def get_project_git_details(project_id):
    """ Fetches git details for a given project ID (assuming they are stored or configured). """
    # Placeholder: In a real app, these might come from config or the projects table
    # For now, returning dummy data or data from environment variables
    # You might need to add columns to the 'projects' table for these
    return {
        "git_url": "https://github.com/BenMacKenzie/db-model-trainer",
        "git_provider": "gitHub",
        "git_branch": "main",
        "notebook_path": "notebooks/01_Build_Model"
    }

def get_dataset_name_by_id(dataset_id):
    """Fetch the name of a dataset given its ID."""
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT name FROM datasets WHERE id = %s", (dataset_id,))
            result = cur.fetchone()
            return result['name'] if result else "Unknown Dataset"
    except Exception as e:
        print(f"Error fetching dataset name for ID {dataset_id}: {e}")
        return "Error Fetching Name"
    finally:
        if conn:
            conn.close()

def get_dataset_name_by_training_table(training_table):
    """Fetch the name of a dataset given its training_table_name."""
    conn = None
    if not training_table: # Handle cases where the param might be missing
        return "Training Table Param Missing"
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Use exact match since the materialized name should be unique and stored
            cur.execute("SELECT name FROM datasets WHERE training_table_name = %s", (training_table,))
            result = cur.fetchone()
            return result['name'] if result else "Unknown Dataset (Table Mismatch?)"
    except Exception as e:
        print(f"Error fetching dataset name for training table {training_table}: {e}")
        return "Error Fetching Name"
    finally:
        if conn:
            conn.close()

def get_dataset_name_by_job_id(job_id):
    """Fetches the dataset name associated with a given Databricks Job ID 
       by looking up the job_id in the training table."""
    conn = None
    if not job_id:
        return "Job ID Missing in Run"
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Find the distinct dataset_id linked to this job_id
            cur.execute("SELECT DISTINCT dataset_id FROM training WHERE job_id = %s", (job_id,))
            training_record = cur.fetchone()
            
            if not training_record:
                return "Job ID Not Found in DB"
                
            dataset_id = training_record['dataset_id']
            
            # Now fetch the dataset name using the found dataset_id
            cur.execute("SELECT name FROM datasets WHERE id = %s", (dataset_id,))
            dataset_record = cur.fetchone()
            
            return dataset_record['name'] if dataset_record else "Dataset Not Found"

    except Exception as e:
        print(f"Error fetching dataset name for job ID {job_id}: {e}")
        return "Error Fetching Name"
    finally:
        if conn:
            conn.close()
