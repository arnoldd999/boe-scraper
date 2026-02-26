import os
import json
import re
import logging
from decimal import Decimal, InvalidOperation
from datetime import datetime

import mysql.connector
from mysql.connector import errorcode

# --- CONFIGURACIÓN DE LOGGING ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("importer")

# --- FUNCIONES AUXILIARES DE PARSEO ---

def parse_money(text: str) -> Decimal | None:
    """Convierte un texto como '1.234,56 €' a un Decimal para la BD."""
    if not text or not isinstance(text, str):
        return None
    try:
        # Limpiar el texto: quitar símbolo de euro, espacios, y usar punto como separador de miles
        cleaned_text = text.replace('€', '').strip().replace('.', '').replace(',', '.')
        return Decimal(cleaned_text)
    except (InvalidOperation, ValueError):
        logger.warning(f"No se pudo convertir a Decimal: '{text}'")
        return None

def parse_datetime(text: str) -> str | None:
    """Extrae y formatea una fecha ISO de un texto como '... (ISO: 2026-02-05T18:00:00+01:00)'."""
    if not text or not isinstance(text, str):
        return None
    match = re.search(r'\(ISO:\s*([^)]+)\)', text)
    if match:
        iso_date_str = match.group(1)
        try:
            # Intenta parsear para validar, luego devuelve el string en formato para MySQL
            dt = datetime.fromisoformat(iso_date_str)
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        except ValueError:
            logger.warning(f"Fecha ISO no válida encontrada: '{iso_date_str}'")
            return None
    return None

def get_safe(data: dict, *keys, default=None):
    """Navega de forma segura por un diccionario anidado para evitar errores si una clave no existe."""
    for key in keys:
        if not isinstance(data, dict) or key not in data:
            return default
        data = data[key]
    return data

# --- LÓGICA PRINCIPAL DEL IMPORTADOR ---

def main():
    try:
        # Gestionar el puerto de forma segura (por defecto 3306 si no está definido o está vacío)
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
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            logger.error("❌ Error de acceso a la BD: usuario o contraseña incorrectos.")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            logger.error(f"❌ La base de datos '{os.getenv('DB_NAME')}' no existe.")
        else:
            logger.error(f"❌ Error de conexión a la BD: {err}")
        exit(1) # Termina el script si no hay conexión

    cursor = db_connection.cursor()
    jsonl_file = os.getenv("OUTPUT_DETAIL_JSONL", "subastas_detalle.jsonl")
    
    logger.info(f"Iniciando importación desde '{jsonl_file}'...")

    with open(jsonl_file, "r", encoding="utf-8") as f:
        for line in f:
            try:
                data = json.loads(line)
                if "error" in data:
                    logger.warning(f"Omitiendo registro con error para URL: {data.get('url')}")
                    continue

                # --- Iniciar transacción para esta subasta ---
                db_connection.start_transaction()

                # 1. UPSERT en la tabla `subastas`
                info_gen = get_safe(data, "Información general", default={})
                identificador = get_safe(info_gen, "Identificador")
                if not identificador:
                    logger.warning(f"Registro sin identificador omitido. URL: {data.get('url')}")
                    continue

                # Extraer el estado de la subasta (necesitarás añadirlo a tu scraper si no está)
                # Como placeholder, lo buscamos en la pestaña de información general
                estado_subasta = get_safe(info_gen, "Estado de la subasta", default="Desconocido")

                sql_subasta = """
                    INSERT INTO subastas (
                        identificador, url, tipo_subasta, estado_subasta,
                        cuenta_expediente, fecha_inicio, fecha_conclusion, cantidad_reclamada,
                        numero_lotes, anuncio_boe
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        url = VALUES(url),
                        tipo_subasta = VALUES(tipo_subasta),
                        estado_subasta = VALUES(estado_subasta),
                        fecha_inicio = VALUES(fecha_inicio),
                        fecha_conclusion = VALUES(fecha_conclusion),
                        cantidad_reclamada = VALUES(cantidad_reclamada),
                        numero_lotes = VALUES(numero_lotes),
                        anuncio_boe = VALUES(anuncio_boe),
                        updated_at = CURRENT_TIMESTAMP;
                """
                
                values_subasta = (
                    identificador,
                    get_safe(data, "url"),
                    get_safe(info_gen, "Tipo de subasta"),
                    estado_subasta,
                    get_safe(info_gen, "Cuenta expediente"),
                    parse_datetime(get_safe(info_gen, "Fecha de inicio")),
                    parse_datetime(get_safe(info_gen, "Fecha de conclusión")),
                    parse_money(get_safe(info_gen, "Cantidad reclamada")),
                    int(get_safe(info_gen, "Lotes", default='0').replace('Sin lotes', '0')),
                    get_safe(info_gen, "Anuncio BOE")
                )
                
                cursor.execute(sql_subasta, values_subasta)
                
                # Obtenemos el ID de la subasta, ya sea nueva o actualizada
                cursor.execute("SELECT id FROM subastas WHERE identificador = %s", (identificador,))
                result = cursor.fetchone()
                subasta_id = result[0] if result else None

                if not subasta_id:
                    logger.error(f"No se pudo obtener el ID para la subasta {identificador}. Revirtiendo.")
                    db_connection.rollback()
                    continue
                
                # --- Aquí iría la lógica para las demás tablas (autoridades_gestoras, lotes, bienes, etc.) ---
                # --- usando el `subasta_id` que acabamos de obtener. ---
                
                db_connection.commit()
                logger.info(f"Subasta {identificador} procesada (ID: {subasta_id}).")

            except (json.JSONDecodeError, mysql.connector.Error, Exception) as e:
                logger.error(f"Error procesando línea o con la BD: {e}")
                db_connection.rollback()

    cursor.close()
    db_connection.close()
    logger.info("✅ Importación finalizada.")

if __name__ == "__main__":
    main()