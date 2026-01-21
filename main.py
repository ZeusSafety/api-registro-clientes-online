import functions_framework
import logging
import requests
import pymysql
import json
import os
from datetime import datetime

# --- CONFIGURACIÓN DE APIS EXTERNAS ---
API_TOKEN_VERIFY = "https://api-verificacion-token-2946605267.us-central1.run.app"

# --- FUNCIÓN DE CONEXIÓN A BASE DE DATOS ---
def get_connection():
    try:
        conn = pymysql.connect(
            user="zeussafety-2024",
            password="ZeusSafety2025",
            db="Zeus_Safety_Data_Integration",
            unix_socket="/cloudsql/stable-smithy-435414-m6:us-central1:zeussafety-2024",
            cursorclass=pymysql.cursors.DictCursor
        )
        with conn.cursor() as cursor:
            cursor.execute("SET time_zone = '-05:00'")
        return conn
    except Exception as e:
        logging.error(f"Error conectando a la base de datos: {str(e)}")
        raise e

# --- 1. LÓGICA DE VENTAS (NUEVA: CABECERA Y DETALLE) ---
def gestionar_venta_completa(data, headers):
    conn = get_connection()
    try:
        cab = data.get('cabecera')
        detalles = data.get('detalles')
        fecha_auto = datetime.now().strftime('%Y-%m-%d')

        with conn.cursor() as cursor:
            # 1. Insertar Cabecera usando el nombre exacto: N°_COMPR
            sql_cab = """
                INSERT INTO ventas_online 
                (FECHA, ASESOR, CLIENTE, `N°_COMPR`, SALIDA_PEDIDO, REGION, DISTRITO, FORMA_PAGO) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(sql_cab, (
                fecha_auto, 
                cab.get("asesor"), 
                cab.get("cliente"), 
                cab.get("comprobante"), # El valor viene del JSON, pero se guarda en N°_COMPR
                cab.get("salida"), 
                cab.get("region"), 
                cab.get("distrito"), 
                cab.get("forma_pago")
            ))
            
            id_venta = cursor.lastrowid 

            # 2. Insertar Detalles usando el nombre exacto: N°_COMPR
            sql_det = """
                INSERT INTO detalle_ventas 
                (LINEA, CANAL_VENTA, `N°_COMPR`, CODIGO_PRODUCTO, PRODUCTO, CANTIDAD, 
                 UNIDAD_MEDIDA, PRECIO_VENTA, DELIVERY, TOTAL, ID_VENTA, CLASIFICACION, FECHA) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            for it in detalles:
                cursor.execute(sql_det, (
                    it.get("linea"), 
                    it.get("canal"), 
                    cab.get("comprobante"), 
                    it.get("codigo"), 
                    it.get("producto"), 
                    it.get("cantidad"), 
                    it.get("unidad"), 
                    it.get("precio"), 
                    it.get("delivery"), 
                    it.get("total"), 
                    id_venta, 
                    it.get("clasificacion"), 
                    fecha_auto
                ))
            
        conn.commit()
        return (json.dumps({"success": "Venta registrada con éxito", "id": id_venta}), 200, headers)
    except Exception as e:
        conn.rollback()
        logging.error(f"Error detallado: {str(e)}")
        return (json.dumps({"error": f"Fallo en venta: {str(e)}"}), 500, headers)
    finally:
        conn.close()

# --- 2. LÓGICA DE CLIENTES (TU CÓDIGO ORIGINAL CRUD) ---
def extraer(request, headers):
    conn = get_connection()
    id_cliente = request.args.get("id")
    with conn:
        with conn.cursor() as cursor:
            if id_cliente:
                cursor.execute("SELECT * FROM clientes_ventas WHERE ID_CLIENTE = %s", (id_cliente,))
                result = cursor.fetchone()
            else:
                cursor.execute("SELECT * FROM clientes_ventas ORDER BY ID_CLIENTE DESC")
                result = cursor.fetchall()
    return (json.dumps(result, default=str), 200, headers)

def insertar_cliente(data, headers):
    conn = get_connection()
    fecha_auto = datetime.now().strftime('%Y-%m-%d')
    try:
        with conn.cursor() as cursor:
            sql = "INSERT INTO clientes_ventas (FECHA, CLIENTE, TELEFONO, RUC, DNI, REGION, DISTRITO, TIPO_CLIENTE, CANAL_ORIGEN) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
            cursor.execute(sql, (fecha_auto, data.get("cliente"), data.get("telefono"), data.get("ruc"), data.get("dni"), data.get("region"), data.get("distrito"), data.get("tipo_cliente"), data.get("canal_origen")))
        conn.commit()
        return (json.dumps({"success": "Cliente registrado", "id": cursor.lastrowid}), 200, headers)
    except Exception as e:
        return (json.dumps({"error": str(e)}), 500, headers)
    finally:
        conn.close()

def actualizar_cliente(data, headers):
    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            sql = "UPDATE clientes_ventas SET CLIENTE=%s, TELEFONO=%s, RUC=%s, DNI=%s, REGION=%s, DISTRITO=%s, TIPO_CLIENTE=%s, CANAL_ORIGEN=%s WHERE ID_CLIENTE=%s"
            cursor.execute(sql, (data.get("cliente"), data.get("telefono"), data.get("ruc"), data.get("dni"), data.get("region"), data.get("distrito"), data.get("tipo_cliente"), data.get("canal_origen"), data.get("id")))
        conn.commit()
        return (json.dumps({"message": "Cliente actualizado"}), 200, headers)
    except Exception as e:
        return (json.dumps({"error": str(e)}), 500, headers)
    finally:
        conn.close()

# --- 3. PUNTO DE ENTRADA PRINCIPAL (EL CEREBRO) ---
@functions_framework.http
def registro_clientes_online(request):
    headers = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'POST, GET, PUT, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type, Authorization',
    }
    if request.method == "OPTIONS": return ("", 204, headers)
    
    # Seguridad
    auth_header = request.headers.get("Authorization")
    if not auth_header: return (json.dumps({"error": "No Token"}), 401, headers)
    
    try:
        resp_auth = requests.post(API_TOKEN_VERIFY, headers={"Authorization": auth_header}, timeout=10)
        if resp_auth.status_code != 200: return (json.dumps({"error": "Token inválido"}), 401, headers)
    except: return (json.dumps({"error": "Auth Error"}), 503, headers)

    # Enrutamiento
    if request.method == "GET":
        return extraer(request, headers)
    
    elif request.method == "POST":
        data = request.get_json()
        # Si el JSON trae 'cabecera', el sistema sabe que es el formulario de VENTAS
        if "cabecera" in data:
            return gestionar_venta_completa(data, headers)
        # Si no, sabe que es el formulario original de CLIENTES
        else:
            return insertar_cliente(data, headers)
            
    elif request.method == "PUT":
        return actualizar_cliente(request.get_json(), headers)

    return (json.dumps({"error": "Método no permitido"}), 405, headers)