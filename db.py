import os
import pyodbc
from dotenv import load_dotenv

load_dotenv()

def get_conn() -> pyodbc.Connection:
    server = os.getenv("AZURE_SQL_SERVER")
    database = os.getenv("AZURE_SQL_DATABASE")
    username = os.getenv("AZURE_SQL_USERNAME")
    password = os.getenv("AZURE_SQL_PASSWORD")
    driver = os.getenv("AZURE_SQL_DRIVER", "ODBC Driver 18 for SQL Server")

    server = os.getenv("AZURE_SQL_SERVER")
    server_tcp = f"tcp:{server},1433"

    if not all([server, database, username, password]):
        missing = [k for k in ["AZURE_SQL_SERVER", "AZURE_SQL_DATABASE", "AZURE_SQL_USERNAME", "AZURE_SQL_PASSWORD"]
                   if not os.getenv(k)]
        raise RuntimeError(f"Missing environment variables: {', '.join(missing)}")

    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server_tcp};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )


    return pyodbc.connect(conn_str)

