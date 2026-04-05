import time

import pandas as pd


def _detect_driver() -> str:
    """Return the best available ODBC driver for SQL Server.

    Queries pyodbc for installed drivers and picks the one with the highest
    version number (e.g. Driver 18 > 17 > 13).  Falls back to
    'ODBC Driver 18 for SQL Server' if pyodbc is not installed or no
    matching driver is found on this machine.
    """
    try:
        import re
        import pyodbc
        candidates = [
            d for d in pyodbc.drivers()
            if "ODBC Driver" in d and "SQL Server" in d
        ]
        if candidates:
            def _version(name: str) -> int:
                m = re.search(r"(\d+)", name)
                return int(m.group(1)) if m else 0
            return max(candidates, key=_version)
    except Exception:
        pass
    return "ODBC Driver 18 for SQL Server"


_DRIVER = _detect_driver()

# Retry configuration for transient network / connection errors
_MAX_RETRIES = 2          # 3 total attempts (1 initial + 2 retries)
_RETRY_BASE_DELAY = 1.0   # seconds; doubles each retry (1 s, 2 s)

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


def _with_retry(fn):
    """Execute fn(), retrying on pyodbc errors with exponential backoff.

    Makes up to _MAX_RETRIES additional attempts after the initial failure,
    waiting _RETRY_BASE_DELAY * 2^attempt seconds between each try.
    Re-raises the final exception if all attempts fail.
    """
    import pyodbc

    last_exc: Exception | None = None
    for attempt in range(_MAX_RETRIES + 1):
        try:
            return fn()
        except pyodbc.Error as exc:
            last_exc = exc
            if attempt < _MAX_RETRIES:
                time.sleep(_RETRY_BASE_DELAY * (2 ** attempt))
    raise last_exc


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
    Retries up to _MAX_RETRIES times on transient pyodbc errors.
    Raises on permanent failure. Always closes the connection.
    """
    import pyodbc  # lazy import — app starts cleanly if pyodbc is not installed

    def _attempt():
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

    return _with_retry(_attempt)


def fetch_table(connection_string: str, table_name: str) -> pd.DataFrame:
    """
    Fetch all rows from a schema-qualified table or view into a DataFrame.
    table_name should be in "schema.name" format (e.g. "dbo.Orders").

    Raises ValueError if table_name is not found in the server's table list,
    preventing unsanitised names from reaching the query string.
    """
    import pyodbc  # lazy import

    # list_tables already uses retry logic for the validation call
    allowed = {t["full_name"] for t in list_tables(connection_string)}
    if table_name not in allowed:
        raise ValueError(
            f"Table {table_name!r} was not found in the accessible tables/views. "
            "Verify the name and your permissions."
        )

    def _attempt():
        conn = pyodbc.connect(connection_string, timeout=30)
        try:
            # Bracket-escape each identifier part (schema and name) to prevent
            # injection through unusual but valid table names.
            parts = table_name.split(".", 1)
            quoted = ".".join(f"[{p.replace(']', ']]')}]" for p in parts)
            return pd.read_sql(f"SELECT * FROM {quoted}", conn)
        finally:
            conn.close()

    return _with_retry(_attempt)


def fetch_query(connection_string: str, sql: str) -> pd.DataFrame:
    """
    Execute a custom SQL query and return the results as a DataFrame.
    Retries up to _MAX_RETRIES times on transient pyodbc errors.
    """
    import pyodbc  # lazy import

    def _attempt():
        conn = pyodbc.connect(connection_string, timeout=30)
        try:
            return pd.read_sql(sql, conn)
        finally:
            conn.close()

    return _with_retry(_attempt)
