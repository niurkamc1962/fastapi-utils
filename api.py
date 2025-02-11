from typing import List, Dict
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from os import getenv
from db.database import get_db_connection, get_db_cursor

app = FastAPI()

origins = [
    getenv("ORIGIN"),
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/", tags=["Root"])
async def hello():
    return "Hello, fastapi"


# Endpoint que lee todos los nombres de las tablas de la BD de Siscont
@app.get("/tables", tags=["Database"], response_model=Dict[str, List[str] | int])
async def get_tables():
    """Retornando lista de los nombres de las tablas y el total de tablas"""
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="No se pudo conectar con la bd")

    cursor = get_db_cursor(conn)
    if not cursor:
        conn.close()
        raise HTTPException(
            status_code=500, detail="No se pudo crear el cursor de conexion a la bd"
        )

    try:
        cursor.execute(
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'"
        )
        tables = [row.TABLE_NAME for row in cursor.fetchall()]

        # obteniendo el total de tablas
        total_tables = len(tables)

        return {"tables": tables, "total_tables": total_tables}
    except Exception as e:
        raise HTTPException(status_code=500, details=str(e))
    finally:
        cursor.close()
        conn.close()


from fastapi import FastAPI, HTTPException, Query
from typing import List, Dict, Any

app = FastAPI()


# Endpoint para obtener la estructura de una o varias tablas
from fastapi import FastAPI, HTTPException
from typing import Dict, List, Any
import pyodbc

app = FastAPI()


# Funciones de conexión a la base de datos (reemplaza con tu configuración)
def get_db_connection():
    try:
        conn_str = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "SERVER=tu_servidor;"
            "DATABASE=tu_base_de_datos;"
            "UID=tu_usuario;"
            "PWD=tu_contraseña;"
        )
        conn = pyodbc.connect(conn_str)
        return conn
    except Exception as e:
        print(f"Error al conectar a la base de datos: {e}")
        return None


def get_db_cursor(conn):
    try:
        cursor = conn.cursor()
        return cursor
    except Exception as e:
        print(f"Error al crear el cursor: {e}")
        return None


# Endpoint para leer la estructura de la tabla seleccionada
@app.get("/tables", tags=["Database"], response_model=Dict[str, List[str] | int])
async def get_tables():
    """Retornando lista de los nombres de las tablas y el total de tablas"""
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="No se pudo conectar con la bd")

    cursor = get_db_cursor(conn)
    if not cursor:
        conn.close()
        raise HTTPException(
            status_code=500, detail="No se pudo crear el cursor de conexion a la bd"
        )

    try:
        cursor.execute(
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'"
        )
        tables = [row.TABLE_NAME for row in cursor.fetchall()]

        # obteniendo el total de tablas
        total_tables = len(tables)

        return {"tables": tables, "total_tables": total_tables}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()


@app.get(
    "/table/{table_name}",
    tags=["Database"],
    response_model=Dict[str, List[Dict[str, Any]]],
)
async def get_table_structure(table_name: str):
    """
    Retorna la estructura de la tabla especificada.
    """
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="No se pudo conectar con la bd")

    cursor = get_db_cursor(conn)
    if not cursor:
        conn.close()
        raise HTTPException(
            status_code=500, detail="No se pudo crear el cursor de conexion a la bd"
        )

    try:
        cursor.execute(
            f"""
            SELECT
                COLUMN_NAME,
                DATA_TYPE,
                CHARACTER_MAXIMUM_LENGTH,
                IS_NULLABLE
            FROM
                INFORMATION_SCHEMA.COLUMNS
            WHERE
                TABLE_NAME = '{table_name}'
        """
        )

        columns = []
        for row in cursor.fetchall():
            columns.append(
                {
                    "column_name": row.COLUMN_NAME,
                    "data_type": row.DATA_TYPE,
                    "max_length": row.CHARACTER_MAXIMUM_LENGTH,
                    "is_nullable": row.IS_NULLABLE,
                }
            )

        if not columns:
            raise HTTPException(
                status_code=404, detail=f"Tabla '{table_name}' no encontrada"
            )

        return {"table_name": table_name, "columns": columns}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()
