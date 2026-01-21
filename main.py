import functions_framework
import logging
import requests
import pymysql
import json
import os
from datetime import datetime

# --- CONFIGURACIÓN DE APIS EXTERNAS ---
API_REGIONES = "https://cotizaciones2026-2946605267.us-central1.run.app/regiones"
API_DISTRITOS = "https://cotizaciones2026-2946605267.us-central1.run.app/distritos"
API_TOKEN_VERIFY = "https://api-verificacion-token-2946605267.us-central1.run.app"

# --- FUNCIÓN DE CONEXIÓN A BASE DE DATOS ---
def get_connection():
    """Establece la conexión usando el socket de Cloud SQL."""
    try:
        conn = pymysql.connect(
            user="zeussafety-2024",
            password="ZeusSafety2025",
            db="Zeus_Safety_Data_Integration",
            unix_socket="/cloudsql/stable-smithy-435414-m6:us-central1:zeussafety-2024",
            cursorclass=pymysql.cursors.DictCursor
        )
        # Forzar zona horaria de Perú
        with conn.cursor() as cursor:
            cursor.execute("SET time_zone = '-05:00'")
        return conn
    except Exception as e:
        logging.error(f"Error conectando a la base de datos: {str(e)}")
        raise e

# --- LÓGICA DE MÉTODOS (CRUD) ---

def extraer(request, headers):
    """Maneja las peticiones GET para listar o buscar por ID."""
    conn = get_connection()
    id_cliente = request.args.get("id")
    
    with conn:
        with conn.cursor() as cursor:
            if id_cliente:
                sql = "SELECT * FROM clientes_ventas WHERE ID_CLIENTE = %s"
                cursor.execute(sql, (id_cliente,))
                result = cursor.fetchone()
            else:
                sql = "SELECT * FROM clientes_ventas ORDER BY ID_CLIENTE DESC"
                cursor.execute(sql)
                result = cursor.fetchall()

    return (json.dumps(result, default=str), 200, headers)

def insertar(request, headers):
    """Maneja las peticiones POST para registrar nuevos clientes."""
    try:
        data = request.get_json()
        if not data:
            return (json.dumps({"error": "Cuerpo de solicitud vacío"}), 400, headers)

        conn = get_connection()
        # Generar fecha automática del servidor (YYYY-MM-DD)
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
            new_id = cursor.lastrowid
        
        return (json.dumps({"success": "Cliente registrado correctamente", "id": new_id}), 200, headers)
    except Exception as e:
        logging.error(f"Error en inserción: {str(e)}")
        return (json.dumps({"error": str(e)}), 500, headers)

def actualizar(request, headers):
    """Maneja las peticiones PUT para actualizar datos existentes."""
    try:
        data = request.get_json()
        id_cliente = data.get("id")
        if not id_cliente:
            return (json.dumps({"error": "ID de cliente es requerido para actualizar"}), 400, headers)

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
        logging.error(f"Error en actualización: {str(e)}")
        return (json.dumps({"error": str(e)}), 500, headers)

# --- PUNTO DE ENTRADA PRINCIPAL ---

@functions_framework.http
def registro_clientes_online(request):
    """Función principal que orquestra la API."""
    
    # 1. Configuración de CORS
    headers = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'POST, GET, PUT, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type, Authorization',
    }

    # Responder a pre-vuelo de CORS
    if request.method == "OPTIONS":
        return ("", 204, headers)
    
    # 2. Verificación de Seguridad (Token)
    auth_header = request.headers.get("Authorization")
    if not auth_header:
        return (json.dumps({"error": "Acceso denegado: Token no proporcionado"}), 401, headers)

    try:
        # Validamos el token contra tu API de verificación
        token_headers = {"Content-Type": "application/json", "Authorization": auth_header}
        resp_auth = requests.post(API_TOKEN_VERIFY, headers=token_headers, timeout=10)
        
        if resp_auth.status_code != 200:
            return (json.dumps({"error": "Token inválido o expirado"}), 401, headers)
    except Exception as e:
        logging.error(f"Error verificando token: {str(e)}")
        return (json.dumps({"error": "Servidor de autenticación no responde"}), 503, headers)

    # 3. Enrutamiento por método HTTP
    if request.method == "GET":
        return extraer(request, headers)
    elif request.method == "POST":
        return insertar(request, headers)
    elif request.method == "PUT":
        return actualizar(request, headers)
    else:
        return (json.dumps({"error": "Método HTTP no permitido"}), 405, headers)