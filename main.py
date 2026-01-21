import functions_framework
import logging
import requests
import pymysql
import json
import os
from datetime import datetime

# Variables de Entorno
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
INSTANCE_CONNECTION_NAME = os.getenv("INSTANCE_CONNECTION_NAME")
API_TOKEN_VERIFY = "https://api-verificacion-token-2946605267.us-central1.run.app"

# APIs Externas
API_REGIONES = "https://cotizaciones2026-2946605267.us-central1.run.app/regiones"
API_DISTRITOS = "https://cotizaciones2026-2946605267.us-central1.run.app/distritos"

def get_connection():
    return pymysql.connect(
        user=DB_USER,
        password=DB_PASSWORD,
        unix_socket=f"/cloudsql/{INSTANCE_CONNECTION_NAME}",
        db=DB_NAME,
        cursorclass=pymysql.cursors.DictCursor,
    )

def extraer(request, headers):
    conn = get_connection()
    id_cliente = request.args.get("id")
    
    with conn:
        with conn.cursor() as cursor:
            if id_cliente:
                # Obtener un cliente específico
                sql = "SELECT * FROM clientes_ventas WHERE ID_CLIENTE = %s"
                cursor.execute(sql, (id_cliente,))
                result = cursor.fetchone()
            else:
                # Listado general
                sql = "SELECT * FROM clientes_ventas ORDER BY ID_CLIENTE DESC"
                cursor.execute(sql)
                result = cursor.fetchall()

    return (json.dumps(result, default=str), 200, headers)

def insertar(request, headers):
    try:
        data = request.get_json()
        conn = get_connection()
        
        # Generar fecha automática (Solo fecha, no hora)
        fecha_auto = datetime.now().strftime('%Y-%m-%d')

        with conn:
            with conn.cursor() as cursor:
                sql = """
                    INSERT INTO clientes_ventas 
                    (FECHA, CLIENTE, TELEFONO, RUC, DNI, REGION, DISTRITO, TIPO_CLIENTE, CANAL_ORIGEN) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor.execute(sql, (
                    fecha_auto,
                    data.get("cliente"),
                    data.get("telefono"),
                    data.get("ruc"),
                    data.get("dni"),
                    data.get("region"),
                    data.get("distrito"),
                    data.get("tipo_cliente"),
                    data.get("canal_origen")
                ))
            conn.commit()
        
        return (json.dumps({"success": "Cliente registrado correctamente", "id": cursor.lastrowid}), 200, headers)
    except Exception as e:
        return (json.dumps({"error": str(e)}), 500, headers)

def actualizar(request, headers):
    try:
        data = request.get_json()
        id_cliente = data.get("id")
        if not id_cliente:
            return (json.dumps({"error": "ID de cliente es requerido"}), 400, headers)

        conn = get_connection()
        with conn:
            with conn.cursor() as cursor:
                sql = """
                    UPDATE clientes_ventas 
                    SET CLIENTE=%s, TELEFONO=%s, RUC=%s, DNI=%s, REGION=%s, 
                        DISTRITO=%s, TIPO_CLIENTE=%s, CANAL_ORIGEN=%s
                    WHERE ID_CLIENTE=%s
                """
                cursor.execute(sql, (
                    data.get("cliente"),
                    data.get("telefono"),
                    data.get("ruc"),
                    data.get("dni"),
                    data.get("region"),
                    data.get("distrito"),
                    data.get("tipo_cliente"),
                    data.get("canal_origen"),
                    id_cliente
                ))
            conn.commit()
            
        return (json.dumps({"message": "Datos actualizados correctamente"}), 200, headers)
    except Exception as e:
        return (json.dumps({"error": str(e)}), 500, headers)

@functions_framework.http
def registro_clientes_online(request):
    # Manejo de CORS
    headers = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'POST, GET, PUT, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type, Authorization',
    }

    if request.method == "OPTIONS":
        return ("", 204, headers)
    
    # Verificación de Seguridad (Token)
    auth_header = request.headers.get("Authorization")
    if not auth_header:
        return (json.dumps({"error": "Token no proporcionado"}), 401, headers)

    try:
        token_headers = {"Content-Type": "application/json", "Authorization": auth_header}
        response = requests.post(API_TOKEN_VERIFY, headers=token_headers, timeout=10)
        
        if response.status_code != 200:
            return (json.dumps({"error": "Token no autorizado o expirado"}), 401, headers)
    except requests.exceptions.RequestException as e:
        return (json.dumps({"error": "Servidor de autenticación no disponible"}), 503, headers)

    # Enrutamiento de métodos
    if request.method == "GET":
        return extraer(request, headers)
    elif request.method == "POST":
        return insertar(request, headers)
    elif request.method == "PUT":
        return actualizar(request, headers)
    else:
        return (json.dumps({"error": "Method not supported"}), 405, headers)