from typing import List, Dict
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from os import getenv
from db.database import get_db_connection, get_db_cursor
import pyodbc

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


# funcion que define la conexion a Siscont
def db_connect_siscont():
    db = {
        "server": "172.20.0.3",
        "port": "1433",
        "database": "S5Principal",
        "user": "sa",
        "password": "S5Principal",
    }
    # preparando la cadena de conexion haciendo uso del driver ODBC 17
    url_siscont = (
        f'DRIVER=ODBC Driver 17 for SQL SERVER;SERVER={db["server"]};PORT={db["port"]};DATABASE'
        f'={db["database"]};TDS_Version=8.0;UID={db["user"]};PWD={db["password"]}'
    )
    try:
        connect_siscont = pyodbc.connect(url_siscont)
        return connect_siscont
    except Exception as e:
        print("Error al crear conexion con sqlserver", e)


@app.get("/conectar", tags=["Database"])
async def check_db_connection():
    try:
        conn = db_connect_siscont()
        if conn:
            conn.close()  # Cierra la conexión después de verificar
            return {
                "status": "success",
                "message": "Conexión a la base de datos establecida correctamente.",
            }
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error al conectar a la base de datos: {str(e)}"
        )


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


@app.get("/table-structure/{table_name}", tags=["Database"])
async def get_table_structure(table_name: str):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="No se pudo conectar con la bd")

    cursor = get_db_cursor(conn)
    if not cursor:
        conn.close()
        raise HTTPException(
            status_code=500, detail="No se pudo crear el cursor de conexion a la bd"
        )

    query = """
    SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = ? """

    cursor.execute(query, table_name)
    columns = cursor.fetchall()

    if not columns:
        raise HTTPException(status_code=404, detail="No existe la tabla")

    # Formateando la respuesta
    table_structure = []
    for column in columns:
        column_info = {
            "column_name": column.COLUMN_NAME,
            "data_type": column.DATA_TYPE,
            "max_length": column.CHARACTER_MAXIMUM_LENGTH,
            "is_nullable": column.IS_NULLABLE,
        }
        table_structure.append(column_info)

    conn.close()
    return {"table_name": table_name, "columns": table_structure}


# @app.get(
#     "/table/{table_name}",
#     tags=["Database"],
#     response_model=Dict[str, List[Dict[str, Any]]],
# )
# async def get_table_structure(table_name: str):
#     """
#     Retorna la estructura de la tabla especificada.
#     """
#     conn = get_db_connection()
#     if not conn:
#         raise HTTPException(status_code=500, detail="No se pudo conectar con la bd")

#     cursor = get_db_cursor(conn)
#     if not cursor:
#         conn.close()
#         raise HTTPException(
#             status_code=500, detail="No se pudo crear el cursor de conexion a la bd"
#         )

#     try:
#         cursor.execute(
#             f"""
#             SELECT
#                 COLUMN_NAME,
#                 DATA_TYPE,
#                 CHARACTER_MAXIMUM_LENGTH,
#                 IS_NULLABLE
#             FROM
#                 INFORMATION_SCHEMA.COLUMNS
#             WHERE
#                 TABLE_NAME = '{table_name}'
#         """
#         )

#         columns = []
#         for row in cursor.fetchall():
#             columns.append(
#                 {
#                     "column_name": row.COLUMN_NAME,
#                     "data_type": row.DATA_TYPE,
#                     "max_length": row.CHARACTER_MAXIMUM_LENGTH,
#                     "is_nullable": row.IS_NULLABLE,
#                 }
#             )

#         if not columns:
#             raise HTTPException(
#                 status_code=404, detail=f"Tabla '{table_name}' no encontrada"
#             )

#         return {"table_name": table_name, "columns": columns}

#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))
#     finally:
#         cursor.close()
#         conn.close()
