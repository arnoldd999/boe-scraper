import asyncio
import json
import logging
import os
from urllib.parse import urljoin

from playwright.async_api import async_playwright

# --- CONFIGURACIÓN DE LOGGING ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# --- CONFIGURACIÓN DEL SCRAPER ---
CONFIG = {
    "HEADLESS": False,
    "URL_BASE": "https://subastas.boe.es/subastas_ava.php",
    "OUTPUT_FILE": "links_castellon.jsonl",
    # Filtros de búsqueda
    "LOCALIDAD": "Castellón de la Plana",  # Texto exacto para el campo Localidad
    "PROVINCIA": "12",  # Código 12 = Castellón
    # Tiempos de espera (ms)
    "TIMEOUT_ESPERA": 3000,
}


def _env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in {"1", "true", "yes", "y", "on"}


def apply_env_config():
    # Permite controlar el comportamiento desde GitHub Actions / cron sin editar el archivo
    CONFIG["HEADLESS"] = _env_bool("HEADLESS", CONFIG["HEADLESS"])
    CONFIG["OUTPUT_FILE"] = os.getenv("OUTPUT_FILE", CONFIG["OUTPUT_FILE"])
    CONFIG["LOCALIDAD"] = os.getenv("BOE_LOCALIDAD", CONFIG["LOCALIDAD"])
    CONFIG["PROVINCIA"] = os.getenv("BOE_PROVINCIA", CONFIG["PROVINCIA"])

    # Por defecto, en automatización conviene sobrescribir el fichero de links
    CONFIG["APPEND_OUTPUT"] = _env_bool("APPEND_OUTPUT", False)


async def recolectar_subastas_paginadas(page):
    # --- FASE 1: BÚSQUEDA ---
    logger.info(f"🔍 Iniciando navegación en {CONFIG['URL_BASE']}...")

    await page.goto(CONFIG["URL_BASE"], wait_until="domcontentloaded")

    # Espera explícita del formulario
    try:
        logger.info("⏳ Esperando a que cargue el formulario...")
        await page.wait_for_selector(".caja.gris", state="visible", timeout=15000)
    except Exception:
        await page.screenshot(path="error_formulario.png")
        logger.error("❌ El formulario no cargó a tiempo.")
        return

    # 1. Seleccionar TIPO DE BIEN: Inmuebles
    # force=True es necesario porque la etiqueta visual (label) tapa el input real
    await page.check("#idTipoBienI", force=True)
    logger.info("✅ Radio button 'Inmuebles' marcado.")

    # 2. Rellenar LOCALIDAD (Campo de texto)
    await page.get_by_label("Localidad").fill(CONFIG["LOCALIDAD"])
    logger.info(f"✅ Localidad '{CONFIG['LOCALIDAD']}' escrita.")

    # 3. Seleccionar PROVINCIA (Desplegable)
    await page.select_option("#BIEN\\.COD_PROVINCIA", CONFIG["PROVINCIA"])
    logger.info(f"✅ Provincia seleccionada (Código {CONFIG['PROVINCIA']}).")

    # Click en Buscar esperando navegación
    logger.info("🖱️ Ejecutando búsqueda...")
    async with page.expect_navigation():
        await page.get_by_role("button", name="Buscar").click()

    # --- FASE 2: BUCLE DE RECOLECCIÓN ---
    logger.info("✅ Resultados cargados. Comenzando extracción...")

    # --- FASE 2: BUCLE DE RECOLECCIÓN ---
    logger.info("✅ Resultados cargados. Comenzando extracción...")

    archivo_salida = CONFIG["OUTPUT_FILE"]

    # Si no vamos en modo append, empezamos limpio para esta ejecución
    if not CONFIG.get("APPEND_OUTPUT", False):
        with open(archivo_salida, "w", encoding="utf-8") as _:
            pass

    vistos_global = set()
    seguimos_buscando = True

    while seguimos_buscando:
        # 1. Localizar el texto de resultados (en tu HTML está en .paginar > p)
        elemento_contador = page.locator(".paginar p").first

        if not await elemento_contador.is_visible():
            logger.error("❌ No se visualiza el contador de resultados (posiblemente no hay resultados).")
            break

        texto_contador = await elemento_contador.inner_text()
        texto_limpio = " ".join(texto_contador.split())
        logger.info(f"📊 Estado: {texto_limpio}")

        # 2. Parseo del string para paginación
        partes = texto_limpio.split(" ")
        try:
            indice_de = partes.index("de")
            num_actual_fin = int(partes[indice_de - 1])
            num_total = int(partes[indice_de + 1])
        except (ValueError, IndexError):
            logger.error("❌ Error interpretando los números de la paginación.")
            break

        # 3. Extraer links (según tu captura: dentro de .resultado-busqueda hay un <a href="...">)
        link_locators = await page.locator(".resultado-busqueda a[href]").all()

        nuevos = 0
        with open(archivo_salida, "a", encoding="utf-8") as f:
            for a in link_locators:
                href = await a.get_attribute("href")
                if not href:
                    continue

                url_completa = urljoin(page.url, href)
                if url_completa in vistos_global:
                    continue
                vistos_global.add(url_completa)

                obj = {
                    "url": url_completa,
                    "provincia": CONFIG["PROVINCIA"],
                    "localidad": CONFIG["LOCALIDAD"],
                }
                f.write(json.dumps(obj, ensure_ascii=False) + "\n")
                nuevos += 1

        logger.info(f"💾 Guardados {nuevos} enlaces de esta página. (total únicos: {len(vistos_global)})")

        # 4. Condición de salida
        if num_actual_fin >= num_total:
            logger.info("🏁 Última página alcanzada.")
            seguimos_buscando = False
        else:
            # 5. Ir a la siguiente página
            boton_siguiente = page.locator(".paginar2 ul li a").last

            if await boton_siguiente.is_visible():
                async with page.expect_navigation():
                    await boton_siguiente.click()
                await asyncio.sleep(0.5)
            else:
                logger.warning("⚠️ No hay botón siguiente aunque los números indican que faltan resultados.")
                break


async def main():
    apply_env_config()

    async with async_playwright() as p:
        logger.info("🚀 Lanzando navegador...")
        browser = await p.chromium.launch(headless=CONFIG["HEADLESS"])
        context = await browser.new_context()
        page = await context.new_page()

        await recolectar_subastas_paginadas(page)

        await browser.close()
        logger.info("👋 Navegador cerrado.")


if __name__ == "__main__":
    asyncio.run(main())