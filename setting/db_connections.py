import pyodbc # type: ignore
from dotenv import load_dotenv  # type: ignore
import os
import platform
from collections import OrderedDict
import psycopg2 # type: ignore
from setting.config import Config


def cursor_ms():
    load_dotenv()

    # Get database connection details
    try:
        # server = os.getenv("Test_server")
        # database = os.getenv("SAP_test_database")
        # username = os.getenv("Test_SQL_username")
        # password = os.getenv("Test_SQL_password")
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
        return conn_, cursor
    except pyodbc.InterfaceError as conn_error:
        print(f"Database Connection Error: {conn_error}")
        raise
    


def ms_query_db(query, args=(), commit=False, fetch_one=False):
    try:
        conn_, cursor = cursor_ms()
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



def query_db(query, args=(), commit=False, fetch_one=False):
    conn = psycopg2.connect(Config.DATABASE_URI)
    cursor = conn.cursor()

    try:
        cursor.execute(query, args)

        # Commit if needed
        if commit:
            conn.commit()


            if fetch_one:
                row = cursor.fetchone()
                if row:
                    columns = [
                        desc[0] for desc in cursor.description
                    ]  # Preserve column order
                    return OrderedDict(zip(columns, row))
            return (
                None  # Default return None if no RETURNING result or fetch_one is False
            )
        else:
            # Fetch results as an ordered dictionary (for SELECT queries)
            columns = [
                desc[0] for desc in cursor.description
            ]  # Get columns as they are in the query

            if fetch_one:
                row = cursor.fetchone()
                if row:
                    return OrderedDict(
                        zip(columns, row)
                    )  # Return first row as an OrderedDict
            else:
                rows = cursor.fetchall()
                return [
                    OrderedDict(zip(columns, row)) for row in rows
                ]  # Return all rows as list of OrderedDicts

    except psycopg2.ProgrammingError as e:
        raise e
    finally:
        cursor.close()
        conn.close()
