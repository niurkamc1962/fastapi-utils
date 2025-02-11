from os import getenv
from dotenv import load_dotenv
import pyodbc


def get_db_connection():
    """Establece la conexion y retorna la conexion a la BD SQL Server"""
    load_dotenv(".env")
    SQL_HOST = getenv("SQL_HOST")
    SQL_USER = getenv("SQL_USER")
    SQL_PASS = getenv("SQL_PASS")
    SQL_PORT = getenv("SQL_PORT")
    SQL_DATABASE = getenv("SQL_DATABASE")

    # asegurando que todas las variables de entorno estan definidas y no vacias
    if not all([SQL_HOST, SQL_USER, SQL_PASS, SQL_PORT, SQL_DATABASE]):
        raise ValueError("Faltan variables de entorno por definir")

    # Preparando la cadena de conexion
    url_siscont = (
        f"DRIVER=ODBC Driver 17 for SQL SERVER;"
        f"SERVER={SQL_HOST};"
        f"PORT={SQL_PORT};"
        f"DATABASE={SQL_DATABASE};"
        f"UID={SQL_USER};"
        f"PWD={SQL_PASS}"
    )
    try:
        conn = pyodbc.connect(url_siscont)
        return conn
    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        print("Error al crear conexion con sqlserver")
        print(ex)
        print(sqlstate)
        return None


def get_db_cursor(conn):
    """Retorna un cursor para ejecutar consultas con la conexion dada"""
    if conn:
        return conn.cursor()
    else:
        return None
