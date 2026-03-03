import asyncio
import json
import logging
import os
import time
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import urljoin

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# ---------------- CONFIG ----------------
INPUT_LINKS_JSONL = os.getenv("INPUT_LINKS_JSONL", "links_subastas.jsonl")
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
BLOCK_HEAVY_RESOURCES = _env_bool("BLOCK_HEAVY_RESOURCES", True)
TEST_LIMIT = 0

# ---------------- LOGGING ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("boe-scraper")

RUN_LOCK = asyncio.Lock()

# ---------------- HELPERS DE LIMPIEZA ----------------
def now_iso_utc() -> str:
    return datetime.now(timezone.utc).isoformat()

def clean_text(s: str) -> str:
    if not s:
        return ""
    return " ".join(s.split()).strip()

def normalize_key(s: str) -> str:
    s = clean_text(s)
    if s.endswith(":"):
        s = s[:-1]
    s = s.lower().replace(" ", "_").replace(".", "").replace("/", "_").replace("á", "a").replace("é", "e").replace("í", "i").replace("ó", "o").replace("ú", "u").replace("ñ", "n")
    return s

def parse_money(text: str) -> Optional[float]:
    if not text:
        return None
    clean = text.replace("€", "").strip()
    clean = clean.replace(".", "").replace(",", ".")
    try:
        return float(clean)
    except ValueError:
        return None

def clean_data_structure(data: Any) -> Any:
    if isinstance(data, dict):
        cleaned = {}
        for k, v in data.items():
            new_key = normalize_key(k)
            if not new_key or new_key in ["ver_mapa", "imagen", ""]:
                continue
            cleaned_v = clean_data_structure(v)
            if cleaned_v not in [None, "", {}, []]:
                cleaned[new_key] = cleaned_v
        return cleaned
    elif isinstance(data, list):
        new_list = []
        for item in data:
            c_item = clean_data_structure(item)
            if c_item not in [None, "", {}, []]:
                new_list.append(c_item)
        return new_list
    elif isinstance(data, str):
        clean_v = clean_text(data)
        if not clean_v:
            return None
        if "€" in data and any(c.isdigit() for c in data):
            money_val = parse_money(data)
            if money_val is not None:
                return money_val
        return clean_v
    else:
        return data

# ---------------- HELPERS DE SCRAPING ----------------
def put_kv(dest: Dict[str, Any], key: str, value: str) -> None:
    key = key.strip()
    value = value.strip()
    if not key or not value:
        return
    if key not in dest:
        dest[key] = value
        return
    prev = dest[key]
    if isinstance(prev, list):
        if value not in prev:
            prev.append(value)
    else:
        if value != prev:
            dest[key] = [prev, value]

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

async def extract_row_kv(tr) -> Dict[str, str]:
    data: Dict[str, str] = {}
    cells = tr.locator("xpath=./th | ./td")
    count = await cells.count()
    if count == 2:
        k = (await cells.nth(0).text_content()) or ""
        v = (await cells.nth(1).text_content()) or ""
        if k.strip() and v.strip():
            data[k.strip()] = v.strip()
    elif count >= 4:
        k1 = (await cells.nth(0).text_content()) or ""
        v1 = (await cells.nth(1).text_content()) or ""
        if k1.strip() and v1.strip():
            data[k1.strip()] = v1.strip()
        k2 = (await cells.nth(2).text_content()) or ""
        v2 = (await cells.nth(3).text_content()) or ""
        if k2.strip() and v2.strip():
            data[k2.strip()] = v2.strip()
    return data

async def extract_dl_kv(root) -> Dict[str, str]:
    data: Dict[str, str] = {}
    dls = root.locator("dl")
    n_dls = await dls.count()
    for i in range(n_dls):
        dl = dls.nth(i)
        dts = dl.locator("dt")
        n_dt = await dts.count()
        for j in range(n_dt):
            dt = dts.nth(j)
            dd = dt.locator("xpath=following-sibling::dd[1]")
            if await dd.count() == 0:
                continue
            k = (await dt.text_content()) or ""
            v = (await dd.first.text_content()) or ""
            if k.strip() and v.strip():
                data[k.strip()] = v.strip()
    return data

async def extract_table_data(table) -> Dict[str, Any]:
    data = {}
    rows = table.locator("tr")
    n_rows = await rows.count()
    for j in range(n_rows):
        tr = rows.nth(j)
        row_kv = await extract_row_kv(tr)
        for k, v in row_kv.items():
            put_kv(data, k, v)
    return data

async def extract_lote_content(page) -> Dict[str, Any]:
    lote_data: Dict[str, Any] = {}
    root = page.locator("#contenido")
    if await root.count() == 0:
        root = page.locator("body")

    # 1. Bienes (h4 + tabla)
    bienes_headers = root.locator("h4").filter(has_text=re.compile(r"^Bien", re.I))
    n_bienes = await bienes_headers.count()
    lista_bienes = []
    
    if n_bienes > 0:
        for i in range(n_bienes):
            h4 = bienes_headers.nth(i)
            titulo_bien = clean_text(await h4.text_content() or "")
            tabla_bien = h4.locator("xpath=following::table[1]")
            if await tabla_bien.count() > 0:
                datos_bien = await extract_table_data(tabla_bien)
                datos_bien["titulo_bien"] = titulo_bien
                lista_bienes.append(datos_bien)
    
    if lista_bienes:
        lote_data["bienes"] = lista_bienes

    # 2. Datos Generales (DLs y Tablas sueltas)
    dl_data = await extract_dl_kv(root)
    lote_data.update(dl_data)
    
    all_tables = root.locator("table")
    n_all = await all_tables.count()
    datos_generales = {}
    for i in range(n_all):
        t_data = await extract_table_data(all_tables.nth(i))
        keys_str = " ".join(t_data.keys()).lower()
        if "valor subasta" in keys_str or "tasación" in keys_str or "depósito" in keys_str:
            datos_generales.update(t_data)
        elif n_bienes == 0:
            datos_generales.update(t_data)
            
    lote_data.update(datos_generales)
    return lote_data

async def extract_mixed_content(page) -> Dict[str, Any]:
    combined_data: Dict[str, Any] = {}
    root = page.locator("#contenido")
    if await root.count() == 0: root = page.locator("body")
    try: await page.wait_for_selector("#contenido, table, dl", timeout=3000)
    except: pass
    try:
        dl_data = await extract_dl_kv(root)
        for k, v in dl_data.items(): put_kv(combined_data, k, v)
    except: pass
    tables = root.locator("table")
    n_tables = await tables.count()
    for i in range(n_tables):
        table = tables.nth(i)
        rows = table.locator("tr")
        n_rows = await rows.count()
        for j in range(n_rows):
            tr = rows.nth(j)
            row_kv = await extract_row_kv(tr)
            for k, v in row_kv.items(): put_kv(combined_data, k, v)
    return combined_data

async def collect_tab_links(page, selector) -> List[Dict[str, str]]:
    try:
        await page.wait_for_selector(selector, timeout=3000)
    except Exception:
        return []

    tabs = page.locator(f"{selector} > li > a")
    n = await tabs.count()
    out = []
    seen = set()

    for i in range(n):
        a = tabs.nth(i)
        try:
            if not await a.is_visible(): continue
            href = await a.get_attribute("href")
            if not href or href.lower().startswith("javascript:") or href == "#": continue
            
            name = clean_text((await a.text_content()) or "")
            if not name: name = clean_text((await a.get_attribute("title")) or "")
            
            if "pujas" in name.lower(): continue

            tab_url = urljoin(page.url, href)
            if tab_url in seen: continue
            seen.add(tab_url)
            out.append({"name": name or f"tab_{i}", "url": tab_url})
        except: continue
    return out

def unique_tab_key(base: str, used: set) -> str:
    base = clean_text(base) or "tab"
    base = normalize_key(base)
    if base not in used:
        used.add(base)
        return base
    i = 2
    while f"{base}_{i}" in used:
        i += 1
    k = f"{base}_{i}"
    used.add(k)
    return k

async def scrape_one(context, url: str) -> Dict[str, Any]:
    page = await context.new_page()
    try:
        await block_resources(page)
        await page.goto(url, timeout=NAV_TIMEOUT_MS, wait_until="domcontentloaded")
        
        raw_result: Dict[str, Any] = {
            "url": url,
            "scraped_at": now_iso_utc(),
        }

        h = page.locator("#contenido h2, #contenido h1, h2, h1, h3").first
        if await h.count() > 0:
            t = clean_text((await h.text_content()) or "")
            if t: raw_result["titulo"] = t

        # 1. Buscar el contenedor principal de tabs: <div id="tabs"> <ul class="navlist">
        main_tabs = await collect_tab_links(page, "#tabs ul.navlist")
        
        if not main_tabs:
            # Fallback: si no hay tabs, extraemos contenido directo
            raw_result["contenido"] = await extract_mixed_content(page)
        else:
            used_keys = set(raw_result.keys())
            
            for tab in main_tabs:
                tab_name_raw = tab.get("name", "")
                tab_key = unique_tab_key(tab_name_raw, used_keys)
                tab_url = tab["url"]
                
                # Navegar a la pestaña principal
                await page.goto(tab_url, timeout=NAV_TIMEOUT_MS, wait_until="domcontentloaded")
                
                # 2. Chequear si existe el contenedor de sub-tabs de lotes: <div id="tabsver"> <ul class="navlistver">
                sub_tabs = await collect_tab_links(page, "#tabsver ul.navlistver")
                
                if sub_tabs:
                    # CASO LOTES: Iterar sobre sub-pestañas
                    lotes_list = []
                    for sub_tab in sub_tabs:
                        sub_url = sub_tab["url"]
                        sub_name = sub_tab["name"]
                        
                        await page.goto(sub_url, timeout=NAV_TIMEOUT_MS, wait_until="domcontentloaded")
                        
                        lote_info = await extract_lote_content(page)
                        lote_info["nombre_lote"] = sub_name
                        lotes_list.append(lote_info)
                    
                    raw_result[tab_key] = lotes_list
                else:
                    # CASO GENERAL: Extraer contenido directo
                    # Si es la pestaña "Lotes" pero no tiene sub-pestañas, igual usamos extract_lote_content
                    if "lote" in tab_key or "bien" in tab_key:
                        raw_result[tab_key] = await extract_lote_content(page)
                    else:
                        raw_result[tab_key] = await extract_mixed_content(page)

        return clean_data_structure(raw_result)

    finally:
        await page.close()

async def worker(context, in_queue: asyncio.Queue, out_queue: asyncio.Queue):
    while True:
        try: url = in_queue.get_nowait()
        except asyncio.QueueEmpty: break
        last_err = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                obj = await scrape_one(context, url)
                keys = set(obj.keys()) - {"url", "scraped_at", "titulo", "error"}
                if not keys: raise Exception("Página vacía")
                await out_queue.put(obj)
                last_err = None
                break
            except PlaywrightTimeoutError as e: last_err = f"Timeout: {e}"
            except Exception as e: last_err = str(e)
            await asyncio.sleep(2 * attempt)
        if last_err:
            logger.error(f"FALLO FINAL en {url}: {last_err}")
            await out_queue.put({"url": url, "scraped_at": now_iso_utc(), "error": last_err})
        in_queue.task_done()

async def writer(out_queue: asyncio.Queue, output_path: str, total_pending: int):
    count = 0
    with open(output_path, "w", encoding="utf-8") as f:
        while True:
            item = await out_queue.get()
            try:
                if item is None: return
                f.write(json.dumps(item, ensure_ascii=False) + "\n")
                count += 1
                if "error" in item: logger.warning(f"[{count}/{total_pending}] ERROR: {item['url']}")
                else:
                    estado = "Desconocido"
                    if "informacion_general" in item and isinstance(item["informacion_general"], dict):
                         estado = item["informacion_general"].get("estado_de_la_subasta", "Desconocido")
                    logger.info(f"[{count}/{total_pending}] OK | {estado} | {item.get('titulo', '')[:30]}")
            finally: out_queue.task_done()

async def main():
    async with RUN_LOCK:
        urls = load_urls_from_links(INPUT_LINKS_JSONL)
        if not urls:
            logger.error(f"No se encontraron URLs en {INPUT_LINKS_JSONL}")
            return
        pending = urls
        if TEST_LIMIT and len(pending) > TEST_LIMIT:
            logger.info(f"MODO PRUEBA: {TEST_LIMIT} de {len(pending)} URLs.")
            pending = pending[:TEST_LIMIT]
        logger.info(f"Iniciando scraping: {len(pending)} URLs.")
        if not pending: return
        in_queue = asyncio.Queue()
        for u in pending: in_queue.put_nowait(u)
        out_queue = asyncio.Queue()
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=HEADLESS)
            context = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36")
            writer_task = asyncio.create_task(writer(out_queue, OUTPUT_DETAIL_JSONL, len(pending)))
            workers = [asyncio.create_task(worker(context, in_queue, out_queue)) for _ in range(MAX_CONCURRENCY)]
            await asyncio.gather(*workers)
            await out_queue.put(None)
            await writer_task
            await browser.close()
        logger.info("Proceso completado.")

if __name__ == "__main__":
    asyncio.run(main())