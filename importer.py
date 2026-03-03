import os
import json
import gzip
import logging
from decimal import Decimal, InvalidOperation
from datetime import datetime
from typing import Any, Dict, Optional

import mysql.connector
from mysql.connector import errorcode

# --- CONFIGURACIÓN DE LOGGING ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("importer")

# --- FUNCIONES AUXILIARES ---

def parse_money(val: Any) -> Optional[Decimal]:
    """Convierte float, int o string limpio a Decimal."""
    if val is None:
        return None
    try:
        return Decimal(str(val))
    except (InvalidOperation, ValueError):
        return None

def parse_datetime(text: str) -> Optional[str]:
    """
    Intenta parsear fechas en varios formatos.
    El scraper devuelve ISO (YYYY-MM-DDTHH:MM:SS...), que es compatible con MySQL.
    """
    if not text or not isinstance(text, str):
        return None
    # Si ya viene en formato ISO limpio del scraper, MySQL lo traga directo.
    # Pero a veces viene sucio. El scraper script2.py ya intenta limpiar, 
    # pero aquí aseguramos compatibilidad.
    try:
        # Intentar formato ISO completo
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    except ValueError:
        pass
    
    return None # Si falla, NULL en BD

def get_safe(data: dict, *keys, default=None):
    """Navega de forma segura por un diccionario anidado."""
    for key in keys:
        if not isinstance(data, dict) or key not in data:
            return default
        data = data[key]
    return data

# --- LÓGICA DE BASE DE DATOS ---

def insert_autoridad(cursor, subasta_id: int, data: dict):
    if not data: return
    sql = """
        INSERT INTO autoridades_gestoras 
        (subasta_id, codigo, descripcion, direccion, telefono, fax, correo_electronico)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    vals = (
        subasta_id,
        get_safe(data, "codigo"),
        get_safe(data, "descripcion"),
        get_safe(data, "direccion"),
        get_safe(data, "telefono"),
        get_safe(data, "fax"),
        get_safe(data, "correo_electronico")
    )
    cursor.execute(sql, vals)

def insert_bienes(cursor, lote_id: int, bienes_list: list):
    if not bienes_list: return
    sql = """
        INSERT INTO bienes 
        (lote_id, titulo_bien, descripcion, direccion, codigo_postal, localidad, provincia, 
         vivienda_habitual, situacion_posesoria, visitable, cargas, referencia_catastral)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    for b in bienes_list:
        vals = (
            lote_id,
            get_safe(b, "titulo_bien"),
            get_safe(b, "descripcion"),
            get_safe(b, "direccion"),
            get_safe(b, "codigo_postal"),
            get_safe(b, "localidad"),
            get_safe(b, "provincia"),
            get_safe(b, "vivienda_habitual"),
            get_safe(b, "situacion_posesoria"),
            get_safe(b, "visitable"),
            get_safe(b, "cargas"),
            get_safe(b, "referencia_catastral") or get_safe(b, "idufir") # A veces viene como IDUFIR
        )
        cursor.execute(sql, vals)

def insert_lotes(cursor, subasta_id: int, lotes_data: Any):
    """
    Maneja la inserción de lotes y sus bienes.
    lotes_data puede ser una lista (varios lotes) o un dict (un solo lote/info general).
    """
    # Normalizamos a lista para iterar
    lista_lotes = []
    if isinstance(lotes_data, list):
        lista_lotes = lotes_data
    elif isinstance(lotes_data, dict):
        # Si es un dict único, lo tratamos como un "Lote Único"
        # A veces el scraper devuelve un dict plano si no hay sub-pestañas.
        lista_lotes = [lotes_data]
    
    sql_lote = """
        INSERT INTO lotes 
        (subasta_id, nombre_lote, descripcion_lote, valor_subasta, valor_de_tasacion, 
         importe_del_deposito, puja_minima, tramos_entre_pujas)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """

    for l in lista_lotes:
        # Insertar Lote
        vals = (
            subasta_id,
            get_safe(l, "nombre_lote") or "Lote Único",
            get_safe(l, "descripcion") or get_safe(l, "descripcion_lote"),
            parse_money(get_safe(l, "valor_subasta")),
            parse_money(get_safe(l, "valor_de_tasacion") or get_safe(l, "tasacion")),
            parse_money(get_safe(l, "importe_del_deposito")),
            get_safe(l, "puja_minima"), # Puede ser texto "Sin puja mínima"
            parse_money(get_safe(l, "tramos_entre_pujas"))
        )
        cursor.execute(sql_lote, vals)
        lote_id = cursor.lastrowid
        
        # Insertar Bienes del Lote
        bienes = get_safe(l, "bienes", default=[])
        # Si no hay lista de bienes explícita, pero hay datos de bien en el propio lote (caso simple),
        # intentamos crear un bien con los datos del lote.
        if not bienes and (get_safe(l, "direccion") or get_safe(l, "localidad")):
             bienes = [l] # El lote es el bien en sí mismo

        insert_bienes(cursor, lote_id, bienes)

def insert_relacionados(cursor, subasta_id: int, data: dict):
    if not data: return
    # A veces relacionados es una lista, a veces un dict
    items = data if isinstance(data, list) else [data]
    
    sql = """
        INSERT INTO relacionados 
        (subasta_id, nombre, nif, direccion, localidad, provincia, pais)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    for r in items:
        vals = (
            subasta_id,
            get_safe(r, "nombre"),
            get_safe(r, "nif"),
            get_safe(r, "direccion"),
            get_safe(r, "localidad"),
            get_safe(r, "provincia"),
            get_safe(r, "pais")
        )
        cursor.execute(sql, vals)

# --- MAIN ---

def main():
    # Configuración de conexión desde variables de entorno
    try:
        port_env = os.getenv("DB_PORT")
        db_port = int(port_env) if port_env and port_env.strip() else 3306

        db_connection = mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASS"),
            database=os.getenv("DB_NAME"),
            port=db_port
        )
        logger.info("✅ Conexión a la base de datos establecida.")
    except mysql.connector.Error as err:
        logger.error(f"❌ Error de conexión a la BD: {err}")
        exit(1)

    cursor = db_connection.cursor()
    
    # Ruta del archivo. En cPanel será algo como /home/user/public_ftp/subastas/subastas_detalle.jsonl.gz
    # Se puede pasar por ENV o hardcodear la ruta relativa si se ejecuta desde el dir correcto.
    jsonl_file = os.getenv("JSONL_FILE_PATH", "subastas_detalle.jsonl.gz")
    
    if not os.path.exists(jsonl_file):
        logger.error(f"❌ No se encuentra el archivo: {jsonl_file}")
        exit(1)

    logger.info(f"Iniciando importación desde '{jsonl_file}'...")

    # Abrir GZIP
    try:
        f = gzip.open(jsonl_file, "rt", encoding="utf-8")
    except Exception as e:
        logger.error(f"Error abriendo archivo gzip: {e}")
        exit(1)

    with f:
        for line in f:
            if not line.strip(): continue
            try:
                data = json.loads(line)
                if "error" in data: continue

                # Datos principales
                info_gen = get_safe(data, "informacion_general", default={})
                identificador = get_safe(info_gen, "identificador")
                
                if not identificador:
                    logger.warning(f"Registro sin identificador. URL: {data.get('url')}")
                    continue

                # --- TRANSACCIÓN ---
                db_connection.start_transaction()

                # 1. UPSERT Subasta
                sql_subasta = """
                    INSERT INTO subastas (
                        identificador, url, titulo, tipo_de_subasta, estado_de_la_subasta,
                        cuenta_expediente, fecha_inicio, fecha_conclusion, cantidad_reclamada,
                        lotes, anuncio_boe, valor_subasta_total_texto
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        url = VALUES(url),
                        titulo = VALUES(titulo),
                        tipo_de_subasta = VALUES(tipo_de_subasta),
                        estado_de_la_subasta = VALUES(estado_de_la_subasta),
                        fecha_inicio = VALUES(fecha_inicio),
                        fecha_conclusion = VALUES(fecha_conclusion),
                        cantidad_reclamada = VALUES(cantidad_reclamada),
                        lotes = VALUES(lotes),
                        anuncio_boe = VALUES(anuncio_boe),
                        valor_subasta_total_texto = VALUES(valor_subasta_total_texto),
                        updated_at = CURRENT_TIMESTAMP;
                """
                
                # Extraer fecha ISO limpia si existe
                f_inicio = get_safe(info_gen, "fecha_de_inicio")
                f_fin = get_safe(info_gen, "fecha_de_conclusion")
                
                # Limpiar texto de fecha (ej: "20-02-2025 ... (ISO: ...)")
                # El scraper script2.py ya debería haber limpiado o dejado el string.
                # Si viene sucio, intentamos extraer ISO con regex o parsear.
                # Asumimos que script2.py hace buen trabajo, pero parse_datetime ayuda.

                vals_subasta = (
                    identificador,
                    get_safe(data, "url"),
                    get_safe(data, "titulo"),
                    get_safe(info_gen, "tipo_de_subasta"),
                    get_safe(info_gen, "estado_de_la_subasta"),
                    get_safe(info_gen, "cuenta_expediente"),
                    parse_datetime(f_inicio),
                    parse_datetime(f_fin),
                    parse_money(get_safe(info_gen, "cantidad_reclamada")),
                    get_safe(info_gen, "lotes"),
                    get_safe(info_gen, "anuncio_boe"),
                    get_safe(info_gen, "valor_subasta") if isinstance(get_safe(info_gen, "valor_subasta"), str) else None
                )
                
                cursor.execute(sql_subasta, vals_subasta)
                
                # Obtener ID (necesario para las tablas hijas)
                cursor.execute("SELECT id FROM subastas WHERE identificador = %s", (identificador,))
                subasta_id = cursor.fetchone()[0]

                # 2. LIMPIEZA DE HIJOS (Estrategia: Borrar y Re-insertar para evitar duplicados/zombies)
                cursor.execute("DELETE FROM autoridades_gestoras WHERE subasta_id = %s", (subasta_id,))
                cursor.execute("DELETE FROM relacionados WHERE subasta_id = %s", (subasta_id,))
                # Borrar lotes borra bienes en cascada por la FK ON DELETE CASCADE
                cursor.execute("DELETE FROM lotes WHERE subasta_id = %s", (subasta_id,))

                # 3. INSERTAR HIJOS
                insert_autoridad(cursor, subasta_id, get_safe(data, "autoridad_gestora"))
                insert_relacionados(cursor, subasta_id, get_safe(data, "relacionados"))
                
                # Lotes (y sus bienes dentro)
                # Buscamos claves que contengan "lote" o "bienes" en el root del json
                # script2.py pone los lotes bajo claves como "lotes", "lote_1", etc.
                # O a veces directamente en "bienes" si es simple.
                
                # Estrategia: Iterar sobre todas las claves del JSON y ver cuáles son lotes
                found_lotes = []
                if "lotes" in data: # Caso ideal: lista de lotes
                    found_lotes = data["lotes"]
                else:
                    # Buscar claves dinámicas "lote_1", "lote_2" o "bienes"
                    for k, v in data.items():
                        if "lote" in k or "bien" in k:
                            if isinstance(v, (dict, list)):
                                found_lotes.append(v)
                
                if found_lotes:
                    insert_lotes(cursor, subasta_id, found_lotes)

                db_connection.commit()
                logger.info(f"Subasta {identificador} procesada OK.")

            except Exception as e:
                logger.error(f"Error procesando línea: {e}")
                db_connection.rollback()

    cursor.close()
    db_connection.close()
    logger.info("✅ Importación finalizada.")

if __name__ == "__main__":
    main()