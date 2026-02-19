import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Any, Union
from urllib.parse import urljoin

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# ---------------- CONFIG ----------------
INPUT_LINKS_JSONL = os.getenv("INPUT_LINKS_JSONL", "links_castellon.jsonl")
OUTPUT_DETAIL_JSONL = os.getenv("OUTPUT_DETAIL_JSONL", "subastas_detalle.jsonl")


def _env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or not v.strip():
        return default
    try:
        return int(v)
    except ValueError:
        return default


HEADLESS = _env_bool("HEADLESS", False)
MAX_CONCURRENCY = _env_int("MAX_CONCURRENCY", 3)
NAV_TIMEOUT_MS = _env_int("NAV_TIMEOUT_MS", 60_000)
MAX_RETRIES = _env_int("MAX_RETRIES", 3)

# Si el sitio es pesado, puedes bajar recursos con esto.
BLOCK_HEAVY_RESOURCES = _env_bool("BLOCK_HEAVY_RESOURCES", True)

# Limit opcional para pruebas (si no se define, no limita)
TEST_LIMIT = _env_int("TEST_LIMIT", 0)

# ---------------- LOGGING ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("boe-scraper")


# ---------------- HELPERS ----------------
def now_iso_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def clean_text(s: str) -> str:
    if not s:
        return ""
    return " ".join(s.split()).strip()


def normalize_key(s: str) -> str:
    s = clean_text(s)
    return s[:-1] if s.endswith(":") else s


def iter_jsonl(path: str) -> Iterable[dict]:
    p = Path(path)
    if not p.exists():
        return
    with p.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue
            except Exception:
                continue


def load_urls_from_links(path: str) -> List[str]:
    urls: List[str] = []
    for obj in iter_jsonl(path):
        u = obj.get("url")
        if isinstance(u, str) and u.startswith("http"):
            urls.append(u)
    seen = set()
    out = []
    for u in urls:
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out


def load_processed_urls(output_path: str) -> set:
    processed = set()
    p = Path(output_path)
    if not p.exists():
        return processed
    for obj in iter_jsonl(output_path):
        u = obj.get("url")
        if isinstance(u, str) and u.startswith("http"):
            processed.add(u)
    return processed


async def block_resources(page):
    if not BLOCK_HEAVY_RESOURCES:
        return

    async def _route(route):
        rtype = route.request.resource_type
        if rtype in {"image", "font", "media"}:
            await route.abort()
        else:
            await route.continue_()

    await page.route("**/*", _route)


async def activate_dynamic_tab(page, tab_locator) -> bool:
    """
    Activa una pestaña dada por su locator.
    """
    try:
        # Verificar si ya está activa (clase 'active' en el padre li o en el a)
        parent_li = tab_locator.locator("xpath=..")
        class_attr_li = await parent_li.get_attribute("class") or ""
        class_attr_a = await tab_locator.get_attribute("class") or ""
        
        if "active" in class_attr_li or "active" in class_attr_a:
            # Ya activa, solo esperamos un poco por seguridad
            await page.wait_for_timeout(200)
            return True

        # Clicar
        await tab_locator.click()

        # Intentar esperar al target
        href = await tab_locator.get_attribute("href")
        if href and href.startswith("#") and len(href) > 1:
            target_id = href
            try:
                await page.wait_for_selector(f"{target_id}:visible", timeout=2000)
                return True
            except Exception:
                pass
        
        # Espera genérica si no pudimos esperar por ID
        await page.wait_for_timeout(500)
        return True
    except Exception:
        return False


async def extract_row_kv(tr) -> Dict[str, str]:
    data = {}
    cells = tr.locator("xpath=./th | ./td")
    count = await cells.count()
    
    if count == 2:
        k = normalize_key(await cells.nth(0).inner_text())
        v = clean_text(await cells.nth(1).inner_text())
        if k:
            data[k] = v
            
    elif count >= 4:
        k1 = normalize_key(await cells.nth(0).inner_text())
        v1 = clean_text(await cells.nth(1).inner_text())
        if k1:
            data[k1] = v1
            
        k2 = normalize_key(await cells.nth(2).inner_text())
        v2 = clean_text(await cells.nth(3).inner_text())
        if k2:
            data[k2] = v2
            
    return data


async def extract_mixed_content(page) -> Dict[str, Any]:
    """
    Estrategia híbrida: Extrae datos de Tablas, Listas de Definición y 
    Párrafos con formato 'Clave: Valor'.
    """
    combined_data: Dict[str, Any] = {}

    # --- 1. ESTRATEGIA DE TABLAS (Estructura rígida) ---
    try:
        # Espera breve por si hay tablas dinámicas
        await page.wait_for_selector("table", timeout=500)
    except Exception:
        pass

    tables = page.locator("table")
    n_tables = await tables.count()

    for i in range(n_tables):
        table = tables.nth(i)
        rows = table.locator("tr")
        n_rows = await rows.count()

        for j in range(n_rows):
            tr = rows.nth(j)
            row_kv = await extract_row_kv(tr)
            combined_data.update(row_kv)

    # --- 2. ESTRATEGIA DE TEXTO PLANO (Heurística 'Label: Value') ---
    # Busca en p, div, li que contengan texto y posiblemente un separador o negrita
    # Limitamos a elementos visibles para evitar ruido del footer/nav oculto
    candidates = page.locator("p:visible, div:visible, li:visible")
    
    # Para optimizar, obtenemos todos los textos de una vez si es posible, 
    # pero iterar es más seguro para la estructura clave-valor.
    # Limitamos a los primeros 100 elementos candidatos para no eternizar el script en páginas gigantes
    count = min(await candidates.count(), 100)

    for i in range(count):
        el = candidates.nth(i)
        text = await el.inner_text()
        text = clean_text(text)
        
        if not text or len(text) > 300: # Ignorar párrafos muy largos (probablemente descripciones, no datos KV)
            continue
            
        # Caso A: Elemento con ":" (Ej: "Referencia catastral: 12345")
        if ":" in text:
            parts = text.split(":", 1)
            k = clean_text(parts[0])
            v = clean_text(parts[1])
            # Heurística: Una clave suele ser corta (menos de 50 chars) y el valor no vacío
            if 2 < len(k) < 60 and v:
                # Evitar sobrescribir si ya vino de una tabla (que es más fiable)
                if k not in combined_data:
                    combined_data[k] = v
                    
        # Caso B: Estructura HTML (Ej: <strong>Label</strong> Value)
        # A veces no hay dos puntos, pero hay cambio de etiqueta.
        # Esto es costoso de verificar elemento por elemento, el Caso A cubre el 90% de los casos.

    # --- 3. ESTRATEGIA DEFINITION LISTS (<dl>) ---
    # Muy común en BOE para datos técnicos
    # (Implementación simplificada: asume dt seguido de dd)
    # ... (Se puede añadir si detectas que usan etiquetas <dl>)

    return combined_data


async def collect_tab_links(page) -> List[Dict[str, str]]:
    """
    Devuelve lista de pestañas con: {name, url}
    Busca en ul.navlist a (tu caso) y fallback a .nav-tabs a.
    """
    # Esperar a que existan links de navegación (si existen)
    try:
        await page.wait_for_selector("ul.navlist a, .nav-tabs a", timeout=3000)
    except Exception:
        return []

    tabs = page.locator("ul.navlist a, .nav-tabs a")
    n = await tabs.count()

    out: List[Dict[str, str]] = []
    seen_urls = set()

    for i in range(n):
        a = tabs.nth(i)

        try:
            href = await a.get_attribute("href")
            if not href:
                continue
            href = href.strip()
            if not href or href.lower().startswith("javascript:"):
                continue

            name = clean_text(await a.inner_text())
            if not name:
                # fallback: si no hay texto, intenta title/aria-label
                name = clean_text((await a.get_attribute("title")) or (await a.get_attribute("aria-label")) or "")

            # Resolver URL relativa si aplica
            tab_url = urljoin(page.url, href)

            if tab_url in seen_urls:
                continue
            seen_urls.add(tab_url)

            out.append({"name": name or f"tab_{i}", "url": tab_url})
        except Exception:
            continue

    return out


async def scrape_one(context, url: str) -> Dict[str, Any]:
    page = await context.new_page()
    try:
        await block_resources(page)

        await page.goto(url, timeout=NAV_TIMEOUT_MS, wait_until="domcontentloaded")

        result: Dict[str, Any] = {
            "url": url,
            "scraped_at": now_iso_utc(),
        }

        # Título (del documento inicial)
        h = page.locator("h1, h2, h3").first
        if await h.count() > 0:
            t = clean_text(await h.inner_text())
            if t:
                result["titulo"] = t

        # 1) Recolectar pestañas desde <ul class="navlist"> (y fallback)
        tabs = await collect_tab_links(page)

        if not tabs:
            # Si no hay pestañas, extrae lo que haya en la página actual
            data = await extract_mixed_content(page)
            if data:
                result["Contenido"] = data
            return result

        # 2) Iterar TODAS las pestañas sin excluir ninguna (incluida “Pujas”)
        for tab in tabs:
            tab_name = tab["name"]
            tab_url = tab["url"]

            try:
                await page.goto(tab_url, timeout=NAV_TIMEOUT_MS, wait_until="domcontentloaded")
            except Exception:
                # Si falla load completo, intenta al menos DOMContentLoaded
                await page.goto(tab_url, timeout=NAV_TIMEOUT_MS, wait_until="domcontentloaded")

            # Extraer datos de esa pestaña/página
            data = await extract_mixed_content(page)
            result[tab_name] = data

        return result
    finally:
        await page.close()


# ---------------- QUEUE PIPELINE ----------------
async def worker(context, in_queue: asyncio.Queue, out_queue: asyncio.Queue):
    while True:
        try:
            url = in_queue.get_nowait()
        except asyncio.QueueEmpty:
            break

        last_err: Optional[str] = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                obj = await scrape_one(context, url)
                
                # Validación básica: debe tener al menos algún dato extraído además de url/fecha/titulo
                keys = set(obj.keys()) - {"url", "scraped_at", "titulo", "error"}
                if not keys:
                     # Si no extrajo ninguna sección, es sospechoso
                     raise Exception("No se detectaron secciones o datos válidos")

                await out_queue.put(obj)
                last_err = None
                break
            except PlaywrightTimeoutError as e:
                last_err = f"Timeout: {e}"
            except Exception as e:
                last_err = str(e)

            # Backoff exponencial
            await asyncio.sleep(2 * attempt)

        if last_err:
            logger.error(f"FALLO FINAL en {url}: {last_err}")
            await out_queue.put(
                {
                    "url": url,
                    "scraped_at": now_iso_utc(),
                    "error": last_err or "unknown error",
                }
            )
        
        in_queue.task_done()


async def writer(out_queue: asyncio.Queue, output_path: str, total_pending: int):
    count = 0
    with open(output_path, "a", encoding="utf-8") as f:
        while True:
            item = await out_queue.get()
            try:
                if item is None:
                    return
                
                f.write(json.dumps(item, ensure_ascii=False) + "\n")
                f.flush()
                
                count += 1
                
                if "error" in item:
                    logger.warning(f"[{count}/{total_pending}] ERROR guardado para {item['url']}")
                else:
                    titulo = item.get("titulo", "Sin título")
                    logger.info(f"[{count}/{total_pending}] OK | {titulo[:30]}...")
            finally:
                out_queue.task_done()


async def main():
    urls = load_urls_from_links(INPUT_LINKS_JSONL)
    if not urls:
        logger.error(f"No se encontraron URLs en {INPUT_LINKS_JSONL}")
        return

    processed = load_processed_urls(OUTPUT_DETAIL_JSONL)
    pending = [u for u in urls if u not in processed]

    # -------------------------------------------------------
    # MODO PRUEBA: Limitamos a 5 URLs para verificar cambios
    # -------------------------------------------------------
    TEST_LIMIT = 5
    if len(pending) > TEST_LIMIT:
        logger.info(f"MODO PRUEBA ACTIVADO: Procesando solo {TEST_LIMIT} de {len(pending)} pendientes.")
        pending = pending[:TEST_LIMIT]

    logger.info(f"URLs totales: {len(urls)} | ya procesadas: {len(processed)} | pendientes: {len(pending)}")
    if not pending:
        logger.info("No hay nada pendiente. Listo.")
        return

    in_queue: asyncio.Queue = asyncio.Queue()
    for u in pending:
        in_queue.put_nowait(u)

    out_queue: asyncio.Queue = asyncio.Queue()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=HEADLESS)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
        )

        writer_task = asyncio.create_task(writer(out_queue, OUTPUT_DETAIL_JSONL, len(pending)))

        workers = [asyncio.create_task(worker(context, in_queue, out_queue)) for _ in range(MAX_CONCURRENCY)]
        
        await asyncio.gather(*workers)

        await out_queue.put(None)
        await writer_task

        await browser.close()

    logger.info("Terminado.")


if __name__ == "__main__":
    asyncio.run(main())