import pandas as pd


_DRIVER = "ODBC Driver 18 for SQL Server"

_SQL_TEMPLATE = (
    "Driver={{{driver}}};"
    "Server=tcp:{server}.database.windows.net,1433;"
    "Database={database};"
    "UID={username};"
    "PWD={password};"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
)

_AAD_TEMPLATE = (
    "Driver={{{driver}}};"
    "Server=tcp:{server}.database.windows.net,1433;"
    "Database={database};"
    "Authentication=ActiveDirectoryInteractive;"
    "Encrypt=yes;"
)

_TABLE_QUERY = """
SELECT TABLE_SCHEMA + '.' + TABLE_NAME AS full_name, TABLE_TYPE
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_TYPE IN ('BASE TABLE', 'VIEW')
ORDER BY TABLE_TYPE, TABLE_SCHEMA, TABLE_NAME
"""


def build_connection_string(
    server: str,
    database: str,
    auth: str,
    username: str = "",
    password: str = "",
) -> str:
    """
    Build a pyodbc connection string for Azure SQL Server.

    auth: "sql" for SQL authentication, "aad" for AAD Interactive.
    Raises ValueError for missing required fields.
    """
    if not server or not database:
        raise ValueError("Server and database are required.")

    if auth == "sql":
        if not username or not password:
            raise ValueError("Username and password are required for SQL authentication.")
        return _SQL_TEMPLATE.format(
            driver=_DRIVER,
            server=server,
            database=database,
            username=username,
            password=password,
        )
    elif auth == "aad":
        return _AAD_TEMPLATE.format(driver=_DRIVER, server=server, database=database)
    else:
        raise ValueError(f"Unknown auth type: {auth!r}. Expected 'sql' or 'aad'.")


def list_tables(connection_string: str) -> list:
    """
    Connect to SQL Server and return all accessible tables and views.

    Returns a list of dicts: [{"full_name": "dbo.Orders", "table_type": "BASE TABLE"}, ...]
    Raises on connection failure. Always closes the connection.
    """
    import pyodbc  # lazy import — app starts cleanly if pyodbc is not installed

    conn = pyodbc.connect(connection_string, timeout=30)
    try:
        cursor = conn.cursor()
        cursor.execute(_TABLE_QUERY)
        return [
            {"full_name": row.full_name, "table_type": row.TABLE_TYPE}
            for row in cursor.fetchall()
        ]
    finally:
        conn.close()


def fetch_table(connection_string: str, table_name: str) -> pd.DataFrame:
    """
    Fetch all rows from a schema-qualified table or view into a DataFrame.
    table_name should be in "schema.name" format (e.g. "dbo.Orders").
    """
    import pyodbc  # lazy import

    conn = pyodbc.connect(connection_string, timeout=30)
    try:
        return pd.read_sql(f"SELECT * FROM {table_name}", conn)
    finally:
        conn.close()


def fetch_query(connection_string: str, sql: str) -> pd.DataFrame:
    """
    Execute a custom SQL query and return the results as a DataFrame.
    """
    import pyodbc  # lazy import

    conn = pyodbc.connect(connection_string, timeout=30)
    try:
        return pd.read_sql(sql, conn)
    finally:
        conn.close()
