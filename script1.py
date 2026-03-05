import asyncio
import json
import logging
import os
from urllib.parse import urljoin

from playwright.async_api import async_playwright

# --- CONFIGURACIÓN DE LOGGING ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("boe-collector")

# --- CONSTANTES ---
# Códigos de provincias (01-52)
ALL_PROVINCIAS = [f"{i:02d}" for i in range(1, 53)]

# --- CONFIGURACIÓN DEL SCRAPER (VALORES POR DEFECTO) ---
CONFIG = {
    "HEADLESS": False,  # Poner en True para que no se abra el navegador
    "URL_BASE": "https://subastas.boe.es/subastas_ava.php",
    "OUTPUT_FILE": "links_subastas.jsonl",
    
    # --- FILTROS ACTIVOS ---
    # Si dejas PROVINCIAS vacío [], buscará en TODAS (01-52).
    # Para pruebas, puedes poner una lista: ["46", "12"] (Valencia, Castellón)
    "PROVINCIAS": [], 
    
    "ESTADOS": ["EJ", "PU"],      # EJ=Celebrándose, PU=Próxima Apertura
    "TIPOS_BIEN": ["I", "V"],     # I=Inmuebles, V=Vehículos
    "TIMEOUT_ESPERA": 3000,
}


def _env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in {"1", "true", "yes", "y", "on"}


def apply_env_config():
    """
    Sobrescribe la configuración por defecto SI existen variables de entorno.
    Si no existen (ejecución local), se mantienen los valores de CONFIG definidos arriba.
    """
    CONFIG["HEADLESS"] = _env_bool("HEADLESS", CONFIG["HEADLESS"])
    
    env_output = os.getenv("LINKS_OUTPUT_FILE")
    if env_output:
        CONFIG["OUTPUT_FILE"] = env_output
    
    # Provincias
    prov_env = os.getenv("BOE_PROVINCIA")
    if prov_env is not None: # Solo si la variable existe
        if not prov_env or prov_env.upper() == "ALL":
            CONFIG["PROVINCIAS"] = ALL_PROVINCIAS
        else:
            CONFIG["PROVINCIAS"] = [p.strip() for p in prov_env.split(',') if p.strip()]
    elif not CONFIG["PROVINCIAS"]: # Si no hay ENV y la lista local está vacía -> USAR TODAS
        CONFIG["PROVINCIAS"] = ALL_PROVINCIAS

    # Estados
    estados_str = os.getenv("BOE_ESTADO")
    if estados_str:
        CONFIG["ESTADOS"] = [s.strip() for s in estados_str.split(',') if s.strip()]

    # Tipos de bien
    tipos_bien_str = os.getenv("BOE_TIPO_BIEN")
    if tipos_bien_str:
        CONFIG["TIPOS_BIEN"] = [t.strip().upper() for t in tipos_bien_str.split(',') if t.strip()]

    CONFIG["APPEND_OUTPUT"] = _env_bool("APPEND_OUTPUT", False)


async def recolectar_subastas_paginadas(page, tipo_bien, estado, provincia, vistos_global, output_file):
    logger.info(f"🔎 Buscando: Prov={provincia} | Tipo={tipo_bien} | Estado={estado}")

    try:
        await page.goto(CONFIG["URL_BASE"], wait_until="domcontentloaded")
        
        # Esperar a que cargue el formulario
        try:
            await page.wait_for_selector(".caja.gris", state="visible", timeout=5000)
        except Exception:
            logger.error("❌ No se cargó el formulario de búsqueda.")
            return

        # --- APLICAR FILTROS ---

        # 1. TIPO DE BIEN (Inmuebles/Vehículos)
        # Usamos selectores CSS directos a los IDs de los inputs
        tipo_bien_map = {"I": "#idTipoBienI", "V": "#idTipoBienV"}
        if tipo_bien in tipo_bien_map:
            # Forzamos el click aunque esté oculto o cubierto por el label
            await page.locator(tipo_bien_map[tipo_bien]).click(force=True)
        else:
            logger.warning(f"Tipo de bien '{tipo_bien}' no soportado. Saltando.")
            return

        # 2. ESTADO DE LA SUBASTA
        estado_map = {"EJ": "#idEstadoEJ", "PU": "#idEstadoPU"}
        estado_nombres = {"EJ": "Celebrándose", "PU": "Próxima Apertura"}
        if estado in estado_map:
            await page.locator(estado_map[estado]).click(force=True)
        else:
            logger.warning(f"Estado '{estado}' no soportado. Saltando.")
            return

        # 3. PROVINCIA
        # Selector escapado para ID con puntos
        await page.select_option("#BIEN\\.COD_PROVINCIA", provincia)

        # 4. RESULTADOS POR PÁGINA: 500
        # Intentamos seleccionar por ID #mostrar (el que me pasaste)
        try:
            await page.select_option("#mostrar", "500")
        except Exception:
            # Fallback por si acaso
            pass

        # 5. CLICK EN BUSCAR
        # Esperamos navegación explícitamente
        async with page.expect_navigation():
            await page.get_by_role("button", name="Buscar").click()

    except Exception as e:
        logger.error(f"❌ Error configurando búsqueda para Prov={provincia}: {e}")
        return

    # --- BUCLE DE PAGINACIÓN ---
    seguimos_buscando = True
    page_num = 1
    
    while seguimos_buscando:
        # Chequeo rápido de "Sin resultados"
        if await page.locator("text='No se han encontrado resultados'").count() > 0:
            logger.info("ℹ️ Sin resultados.")
            break
        
        # Chequeo de "Demasiados resultados"
        if await page.locator("text='La consulta devuelve demasiados resultados'").count() > 0:
            logger.warning("⚠️ Demasiados resultados. Se recomienda filtrar más.")
            break

        # Extraer enlaces
        # Buscamos dentro de la clase .resultado-busqueda los <a> con href
        link_locators = await page.locator(".resultado-busqueda a[href]").all()
        
        nuevos = 0
        with open(output_file, "a", encoding="utf-8") as f:
            for a in link_locators:
                href = await a.get_attribute("href")
                if not href or "detalleSubasta" not in href:
                    continue

                url_completa = urljoin(page.url, href)
                
                if url_completa in vistos_global:
                    continue
                vistos_global.add(url_completa)

                obj = {
                    "url": url_completa,
                    "provincia": provincia,
                    "tipo_bien": tipo_bien,
                    "estado": estado,
                    "estado_nombre": estado_nombres.get(estado, "Desconocido"),
                    "scraped_at": os.getenv("RUN_TIMESTAMP", "")
                }
                f.write(json.dumps(obj, ensure_ascii=False) + "\n")
                nuevos += 1

        logger.info(f"   📄 Pág {page_num}: {nuevos} links nuevos.")

        # Navegar a siguiente página
        # Buscamos el enlace que tenga title="Página siguiente" O texto "Siguiente"
        boton_siguiente = page.locator("a[title='Página siguiente']")
        if await boton_siguiente.count() == 0:
             boton_siguiente = page.locator("text='Siguiente'")

        if await boton_siguiente.count() > 0 and await boton_siguiente.is_visible():
            async with page.expect_navigation():
                await boton_siguiente.first.click()
            page_num += 1
        else:
            seguimos_buscando = False


async def main():
    # Aplicar configuración (prioridad ENV > Default)
    apply_env_config()

    output_file = CONFIG["OUTPUT_FILE"]
    # Limpiar fichero si no es modo append
    if not CONFIG.get("APPEND_OUTPUT", False):
        with open(output_file, "w", encoding="utf-8") as f:
            f.write("")

    vistos_global = set()
    
    logger.info("🚀 Iniciando recolección.")
    logger.info(f"📋 Provincias a escanear: {len(CONFIG['PROVINCIAS'])}")
    logger.info(f"📋 Tipos de bien: {CONFIG['TIPOS_BIEN']}")
    logger.info(f"📋 Estados: {CONFIG['ESTADOS']}")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=CONFIG["HEADLESS"])
        context = await browser.new_context()
        page = await context.new_page()

        for provincia in CONFIG["PROVINCIAS"]:
            for tipo in CONFIG["TIPOS_BIEN"]:
                for estado in CONFIG["ESTADOS"]:
                    await recolectar_subastas_paginadas(
                        page, tipo, estado, provincia, vistos_global, output_file
                    )

        await browser.close()
    
    logger.info(f"✅ Recolección finalizada. Total enlaces únicos: {len(vistos_global)}")

if __name__ == "__main__":
    asyncio.run(main())