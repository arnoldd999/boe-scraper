import os
import json
import re
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
    if val is None:
        return None
    if isinstance(val, list):
        if not val:
            return None
        val = val[0]
    try:
        return Decimal(str(val))
    except (InvalidOperation, ValueError):
        return None

def _ensure_scalar_string(value: Any) -> Optional[str]:
    """
    Ensures a value is a scalar string or None. If it's a list, it joins the elements.
    """
    if value is None:
        return None
    if isinstance(value, list):
        return ", ".join(map(str, value))
    return str(value)
# --- CONSTANTES ---
# Diccionario para mapear códigos de provincia a nombres
PROVINCES = {
    "01": "Araba/Álava", "02": "Albacete", "03": "Alicante/Alacant", "04": "Almería", "05": "Ávila",
    "06": "Badajoz", "07": "Illes Balears", "08": "Barcelona", "09": "Burgos", "10": "Cáceres",
    "11": "Cádiz", "12": "Castellón/Castelló", "13": "Ciudad Real", "14": "Córdoba", "15": "A Coruña",
    "16": "Cuenca", "17": "Girona", "18": "Granada", "19": "Guadalajara", "20": "Gipuzkoa",
    "21": "Huelva", "22": "Huesca", "23": "Jaén", "24": "León", "25": "Lleida",
    "26": "La Rioja", "27": "Lugo", "28": "Madrid", "29": "Málaga", "30": "Murcia",
    "31": "Navarra", "32": "Ourense", "33": "Asturias", "34": "Palencia", "35": "Las Palmas",
    "36": "Pontevedra", "37": "Salamanca", "38": "Santa Cruz de Tenerife", "39": "Cantabria", "40": "Segovia",
    "41": "Sevilla", "42": "Soria", "43": "Tarragona", "44": "Teruel", "45": "Toledo",
    "46": "Valencia/València", # ¡Aquí está el cambio para Valencia!
    "47": "Valladolid", "48": "Bizkaia", "49": "Zamora", "50": "Zaragoza",
    "51": "Ceuta", "52": "Melilla"
}

def get_province_name(code: str) -> Optional[str]:
    """
    Traduce el código de provincia (ej. "46") a su nombre (ej. "Valencia/València").
    """
    return PROVINCES.get(code)


def parse_datetime(text: Any) -> Optional[str]:
    if isinstance(text, list):
        if not text:
            return None
        text = text[0]
    if not text or not isinstance(text, str):
        return None

    # Extraer la parte ISO si el texto tiene el formato del BOE: "... (ISO: 2026-02-05T18:00:00+01:00)"
    match = re.search(r'\(ISO:\s*([^)]+)\)', text)
    if match:
        text = match.group(1)

    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    except ValueError:
        pass
    return None

def get_safe(data: dict, *keys, default=None):
    for key in keys:
        if not isinstance(data, dict) or key not in data:
            return default
        data = data[key]
    return data

# --- LÓGICA DE BASE DE DATOS (NUEVOS NOMBRES) ---

def insert_autoridad(cursor, subasta_id: int, data: dict):
    if not data: return
    sql = """
        INSERT INTO subastas_autoridad 
        (subasta_id, codigo, descripcion, direccion, telefono, fax, correo_electronico)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    vals = (
        subasta_id,
        _ensure_scalar_string(get_safe(data, "codigo")),
        _ensure_scalar_string(get_safe(data, "descripcion")),
        _ensure_scalar_string(get_safe(data, "direccion")),
        _ensure_scalar_string(get_safe(data, "telefono")),
        _ensure_scalar_string(get_safe(data, "fax")),
        _ensure_scalar_string(get_safe(data, "correo_electronico"))
    )
    cursor.execute(sql, vals)

def insert_items(cursor, lote_id: int, items_list: list):
    if not items_list: return
    sql = """
        INSERT INTO lotes_items 
        (lote_id, titulo_bien, descripcion, direccion, codigo_postal, localidad, provincia, 
         vivienda_habitual, situacion_posesoria, visitable, cargas, referencia_catastral, datos_adicionales)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    known_columns = ['titulo_bien', 'descripcion', 'direccion', 'codigo_postal', 'localidad', 'provincia','vivienda_habitual','situacion_posesoria','visitable','cargas','referencia_catastral','idufir']
    for b in items_list:

        extra_data = {k: v for k, v in b.items() if k not in known_columns}
        vals = (
            lote_id,
            _ensure_scalar_string(get_safe(b, "titulo_bien")),
            _ensure_scalar_string(get_safe(b, "descripcion")),
            _ensure_scalar_string(get_safe(b, "direccion")),
            _ensure_scalar_string(get_safe(b, "codigo_postal")),
            _ensure_scalar_string(get_safe(b, "localidad")),
            _ensure_scalar_string(get_safe(b, "provincia")),
            _ensure_scalar_string(get_safe(b, "vivienda_habitual")),
            _ensure_scalar_string(get_safe(b, "situacion_posesoria")),
            _ensure_scalar_string(get_safe(b, "visitable")),
            _ensure_scalar_string(get_safe(b, "cargas")),
            _ensure_scalar_string(get_safe(b, "referencia_catastral") or get_safe(b, "idufir")),
            json.dumps(extra_data) if extra_data else None
        )
        cursor.execute(sql, vals)

def insert_lotes(cursor, subasta_id: int, lotes_data: Any):
    lista_lotes = []
    if isinstance(lotes_data, list):
        lista_lotes = lotes_data
    elif isinstance(lotes_data, dict):
        lista_lotes = [lotes_data]
    
    sql_lote = """
        INSERT INTO subastas_lotes 
        (subasta_id, nombre_lote, descripcion_lote, valor_subasta, valor_de_tasacion, 
         importe_del_deposito, puja_minima, tramos_entre_pujas)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """

    for l in lista_lotes:
        vals = (
            subasta_id,
            _ensure_scalar_string(get_safe(l, "nombre_lote") or "Lote Único"),
            _ensure_scalar_string(get_safe(l, "descripcion") or get_safe(l, "descripcion_lote")),
            parse_money(get_safe(l, "valor_subasta")),
            parse_money(get_safe(l, "valor_de_tasacion") or get_safe(l, "tasacion")),
            parse_money(get_safe(l, "importe_del_deposito")),
            _ensure_scalar_string(get_safe(l, "puja_minima")),
            parse_money(get_safe(l, "tramos_entre_pujas"))
        )
        cursor.execute(sql_lote, vals)
        lote_id = cursor.lastrowid
        
        bienes = get_safe(l, "bienes", default=[])
        if not bienes and (get_safe(l, "direccion") or get_safe(l, "localidad")):
             bienes = [l]

        insert_items(cursor, lote_id, bienes)

def insert_acreedores(cursor, subasta_id: int, data: dict):
    if not data: return
    items = data if isinstance(data, list) else [data]
    
    sql = """
        INSERT INTO subastas_acreedores 
        (subasta_id, nombre, nif, direccion, localidad, provincia, pais)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    for r in items:
        vals = (
            subasta_id,
            _ensure_scalar_string(get_safe(r, "nombre")),
            _ensure_scalar_string(get_safe(r, "nif")),
            _ensure_scalar_string(get_safe(r, "direccion")),
            _ensure_scalar_string(get_safe(r, "localidad")),
            _ensure_scalar_string(get_safe(r, "provincia")),
            _ensure_scalar_string(get_safe(r, "pais"))
        )
        cursor.execute(sql, vals)

# --- MAIN ---

def main():
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
    jsonl_file = os.getenv("JSONL_FILE_PATH", "subastas_detalle.jsonl.gz")
    
    if not os.path.exists(jsonl_file):
        logger.error(f"❌ No se encuentra el archivo: {jsonl_file}")
        exit(1)

    logger.info(f"Iniciando importación desde '{jsonl_file}'...")

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

                info_gen = get_safe(data, "informacion_general", default={})
                identificador = get_safe(info_gen, "identificador")
                
                if not identificador:
                    continue

                db_connection.start_transaction()

                # 1. UPSERT Subasta
                sql_subasta = """
                    INSERT INTO subastas (
                        identificador, url, titulo, tipo_de_subasta, estado_de_la_subasta, provincia_nombre,
                        cuenta_expediente, fecha_inicio, fecha_conclusion, cantidad_reclamada,
                        lotes, anuncio_boe, valor_subasta_total_texto
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON DUPLICATE KEY UPDATE
                        url = VALUES(url),
                        titulo = VALUES(titulo),
                        tipo_de_subasta = VALUES(tipo_de_subasta),
                        estado_de_la_subasta = VALUES(estado_de_la_subasta),
                        fecha_inicio = VALUES(fecha_inicio),
                        fecha_conclusion = VALUES(fecha_conclusion),
                        provincia_nombre = VALUES(provincia_nombre),
                        cantidad_reclamada = VALUES(cantidad_reclamada),
                        lotes = VALUES(lotes),
                        anuncio_boe = VALUES(anuncio_boe),
                        valor_subasta_total_texto = VALUES(valor_subasta_total_texto),
                        updated_at = CURRENT_TIMESTAMP;
                """
                
                f_inicio = get_safe(info_gen, "fecha_de_inicio")
                f_fin = get_safe(info_gen, "fecha_de_conclusion")
                val_subasta = get_safe(info_gen, "valor_subasta")
                val_subasta_str = str(val_subasta) if val_subasta is not None else None
                
                # Obtener el nombre de la provincia a partir del código
                provincia_code = get_safe(data, "meta_provincia")
                provincia_name = get_province_name(provincia_code)
                
                # Asegurarse de que 'lotes' sea una cadena de texto, no una lista
                lotes_value = get_safe(info_gen, "lotes")
                if isinstance(lotes_value, list):
                    lotes_value = ", ".join(map(str, lotes_value)) # Convierte la lista a una cadena
                elif lotes_value is not None:
                    lotes_value = str(lotes_value) # Asegura que sea una cadena si no es None

                vals_subasta = (
                    _ensure_scalar_string(identificador),
                    _ensure_scalar_string(get_safe(data, "url")),
                    _ensure_scalar_string(get_safe(data, "titulo")),
                    _ensure_scalar_string(get_safe(info_gen, "tipo_de_subasta")),
                    _ensure_scalar_string(get_safe(data, "meta_estado_nombre") or get_safe(info_gen, "estado_de_la_subasta")),
                    _ensure_scalar_string(provincia_name), # Nuevo campo para el nombre de la provincia
                    _ensure_scalar_string(get_safe(info_gen, "cuenta_expediente")),
                    parse_datetime(f_inicio),
                    parse_datetime(f_fin),
                    parse_money(get_safe(info_gen, "cantidad_reclamada")), # Asegura que sea un Decimal o None
                    lotes_value, # Usa el valor de lotes procesado
                    _ensure_scalar_string(get_safe(info_gen, "anuncio_boe")),
                    _ensure_scalar_string(val_subasta_str)
                )
                
                cursor.execute(sql_subasta, vals_subasta)
                
                cursor.execute("SELECT id FROM subastas WHERE identificador = %s", (identificador,))
                subasta_id = cursor.fetchone()[0]

                # 2. LIMPIEZA DE HIJOS (Nuevos nombres)
                cursor.execute("DELETE FROM subastas_autoridad WHERE subasta_id = %s", (subasta_id,))
                cursor.execute("DELETE FROM subastas_acreedores WHERE subasta_id = %s", (subasta_id,))
                cursor.execute("DELETE FROM subastas_lotes WHERE subasta_id = %s", (subasta_id,))

                # 3. INSERTAR HIJOS
                insert_autoridad(cursor, subasta_id, get_safe(data, "autoridad_gestora"))
                insert_acreedores(cursor, subasta_id, get_safe(data, "relacionados"))
                
                found_lotes = []
                if "lotes" in data:
                    found_lotes = data["lotes"]
                else:
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