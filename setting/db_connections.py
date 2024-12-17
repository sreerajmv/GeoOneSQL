import pyodbc
from dotenv import load_dotenv
import os
import platform
from collections import OrderedDict


def ms_query_db(query, args=(), commit=False, fetch_one=False):
    """
    Executes a query on the SQL Server database with optional commit and fetch options.
    """
    # Load environment variables
    load_dotenv()

    # Get database connection details
    try:
        server = os.getenv("pr_Server")
        database = os.getenv("eBiz_database")
        username = os.getenv("pr_SQL_username")
        password = os.getenv("pr_SQL_password")
        if not all([server, database, username, password]):
            raise ValueError(
                "Missing required environment variables for database connection."
            )
    except Exception as env_error:
        print(f"Environment Variable Error: {env_error}")
        raise

    # Determine the driver based on the platform
    driver_name = (
        "SQL Server"
        if platform.system() == "Windows"
        else "/usr/lib64/libmsodbcsql-17.so"
    )

    # Connect to the database
    try:
        conn_ = pyodbc.connect(
            f"DRIVER={driver_name};SERVER={server};DATABASE={database};UID={username};PWD={password}"
        )
        cursor = conn_.cursor()
    except pyodbc.InterfaceError as conn_error:
        print(f"Database Connection Error: {conn_error}")
        raise

    try:
        # Execute the query
        cursor.execute(query, args)

        # Commit changes if required
        if commit:
            conn_.commit()

            if fetch_one:  # Fetch one row if needed
                row = cursor.fetchone()
                if row:
                    columns = [desc[0] for desc in cursor.description]
                    return OrderedDict(zip(columns, row))
            return None  # Default return for commit without fetch_one
        else:
            # Handle SELECT queries
            columns = [desc[0] for desc in cursor.description]
            if fetch_one:
                row = cursor.fetchone()
                if row:
                    return OrderedDict(zip(columns, row))
                return None  # No rows fetched
            else:
                rows = cursor.fetchall()
                return [OrderedDict(zip(columns, row)) for row in rows]

    except pyodbc.ProgrammingError as e:
        print(f"SQL Programming Error: {e}")
        raise
    except pyodbc.Error as sql_error:
        print(f"SQL Error: {sql_error}")
        raise
    finally:
        # Ensure resources are always released
        try:
            cursor.close()
            conn_.close()
        except Exception as cleanup_error:
            print(f"Resource Cleanup Error: {cleanup_error}")
