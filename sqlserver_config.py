import os
from dotenv import load_dotenv
load_dotenv()

# SQL Server connection settings
sql_server = {
    "server": os.getenv("SQL_SERVER", ""),
    "database": os.getenv("SQL_DATABASE", ""),
    "user": os.getenv("SQL_USER"),
    "password": os.getenv("SQL_PASSWORD"),
    "driver": os.getenv("SQL_DRIVER", "SQL Server Native Client 10.0"),
}

if sql_server["user"] is None or sql_server["password"] is None:
    raise EnvironmentError("SQL_USER and SQL_PASSWORD must be set in the environment")
