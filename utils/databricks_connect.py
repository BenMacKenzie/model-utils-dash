import os
from databricks import sql
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_databricks_connection():
    """Establishes a connection to the Databricks SQL warehouse.

    Uses environment variables DATABRICKS_HOST, DATABRICKS_WAREHOUSE_ID,
    and DATABRICKS_TOKEN for connection details.

    Returns:
        databricks.sql.client.Connection: A connection object or None if connection fails.
    """
    host = os.getenv("DATABRICKS_HOST")
    warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID") # Should be the HTTP Path
    http_path=f"/sql/1.0/warehouses/{warehouse_id}"
    token = os.getenv("DATABRICKS_TOKEN")

    # --- Add print statements for debugging ---
    print(f"DEBUG: DATABRICKS_HOST read from env: {host}")
    print(f"DEBUG: DATABRICKS_WAREHOUSE_ID read from env: {http_path}")
    # Optionally print token length or just confirmation, avoid printing the actual token
    print(f"DEBUG: DATABRICKS_TOKEN read from env: {'Set' if token else 'Not Set'}")
    # --- End of debug prints ---

    if not all([host, http_path, token]):
        logger.error("Databricks connection environment variables (DATABRICKS_HOST, DATABRICKS_WAREHOUSE_ID, DATABRICKS_TOKEN) not set.")
        return None

    try:
        connection = sql.connect(
            server_hostname=host,
            http_path=http_path,
            access_token=token
        )
        logger.info("Successfully connected to Databricks SQL Warehouse.")
        return connection
    except Exception as e:
        logger.error(f"Failed to connect to Databricks SQL Warehouse: {e}")
        return None

def execute_sql(connection, sql_query):
    """Executes a SQL query on the given Databricks connection.

    Args:
        connection (databricks.sql.client.Connection): The active Databricks connection.
        sql_query (str): The SQL query string to execute.

    Returns:
        bool: True if execution was successful, False otherwise.
    """
    if not connection:
        logger.error("Cannot execute SQL: No valid Databricks connection.")
        return False

    try:
        with connection.cursor() as cursor:
            logger.info(f"Executing SQL: {sql_query}")
            cursor.execute(sql_query)
            logger.info("SQL executed successfully.")
            # Note: For CTAS, execute doesn't return results directly.
            # We might need different handling if we expect results later.
        return True
    except Exception as e:
        logger.error(f"Failed to execute SQL query '{sql_query}': {e}")
        return False

# Example usage (optional - can be commented out or removed)
# if __name__ == '__main__':
#     conn = get_databricks_connection()
#     if conn:
#         # Example: Create a dummy table (adjust schema/table name as needed)
#         create_table_sql = "CREATE TABLE IF NOT EXISTS default.test_table (id INT, name STRING)"
#         success = execute_sql(conn, create_table_sql)
#         if success:
#             print("Dummy table created (or already exists).")
        
#         # Example: Show tables
#         show_tables_sql = "SHOW TABLES IN default"
#         # Need a different function or modification to fetch results for SELECT/SHOW
#         print(f"Run '{show_tables_sql}' manually in Databricks to verify.")

#         conn.close()
#         print("Connection closed.") 