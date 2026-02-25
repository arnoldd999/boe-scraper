import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
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


def load_links_data(path: str) -> List[Dict[str, str]]:
    links: List[Dict[str, str]] = []
    seen_urls = set()
    for obj in iter_jsonl(path):
        url = obj.get("url")
        if isinstance(url, str) and url.startswith("http") and url not in seen_urls:
            links.append(obj)
            seen_urls.add(url)
    return links


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
        parent_li = tab_locator.locator("xpath=..")
        class_attr_li = await parent_li.get_attribute("class") or ""
        class_attr_a = await tab_locator.get_attribute("class") or ""

        if "active" in class_attr_li or "active" in class_attr_a:
            await page.wait_for_timeout(200)
            return True

        await tab_locator.click()

        href = await tab_locator.get_attribute("href")
        if href and href.startswith("#") and len(href) > 1:
            target_id = href
            try:
                await page.wait_for_selector(f"{target_id}:visible", timeout=2000)
                return True
            except Exception:
                pass

        await page.wait_for_timeout(500)
        return True
    except Exception:
        return False


async def extract_row_kv(tr) -> Dict[str, str]:
    data: Dict[str, str] = {}
    cells = tr.locator("xpath=./th | ./td")
    count = await cells.count()

    # NOTA: text_content() es más rápido y estable que inner_text() para scraping KV
    if count == 2:
        k = normalize_key(clean_text((await cells.nth(0).text_content()) or ""))
        v = clean_text((await cells.nth(1).text_content()) or "")
        if k and v:
            data[k] = v

    elif count >= 4:
        k1 = normalize_key(clean_text((await cells.nth(0).text_content()) or ""))
        v1 = clean_text((await cells.nth(1).text_content()) or "")
        if k1 and v1:
            data[k1] = v1

        k2 = normalize_key(clean_text((await cells.nth(2).text_content()) or ""))
        v2 = clean_text((await cells.nth(3).text_content()) or "")
        if k2 and v2:
            data[k2] = v2

    return data


async def extract_dl_kv(root) -> Dict[str, str]:
    """
    Extrae KV de <dl><dt>Clave</dt><dd>Valor</dd></dl> dentro de un root locator.
    """
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

            k = normalize_key(clean_text((await dt.text_content()) or ""))
            v = clean_text((await dd.first.text_content()) or "")
            if k and v:
                data[k] = v

    return data


async def extract_mixed_content(page) -> Dict[str, Any]:
    """
    Estrategia precisa: extrae KV principalmente de TABLAS y <dl>.
    Evita el barrido general de p/div/li por ruido (menús, avisos, footer).
    Se restringe al contenedor #contenido cuando exista.
    """
    combined_data: Dict[str, Any] = {}

    root = page.locator("#contenido")
    if await root.count() == 0:
        root = page.locator("body")

    # Espera corta a que el contenido "real" exista
    try:
        await page.wait_for_selector("#contenido, table, dl", timeout=5000)
    except Exception:
        pass

    # 1) DL
    try:
        dl_data = await extract_dl_kv(root)
        for k, v in dl_data.items():
            put_kv(combined_data, k, v)
    except Exception:
        pass

    # 2) TABLAS
    tables = root.locator("table")
    n_tables = await tables.count()

    for i in range(n_tables):
        table = tables.nth(i)
        rows = table.locator("tr")
        n_rows = await rows.count()

        for j in range(n_rows):
            tr = rows.nth(j)
            row_kv = await extract_row_kv(tr)
            for k, v in row_kv.items():
                put_kv(combined_data, k, v)

    return combined_data


async def collect_tab_links(page) -> List[Dict[str, str]]:
    """
    Devuelve lista de pestañas con: {name, url}
    Busca en ul.navlist a (Portal BOE) y fallback a .nav-tabs a.
    """
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

            name = clean_text((await a.text_content()) or "")
            if not name:
                name = clean_text((await a.get_attribute("title")) or (await a.get_attribute("aria-label")) or "")

            tab_url = urljoin(page.url, href)
            if tab_url in seen_urls:
                continue
            seen_urls.add(tab_url)

            out.append({"name": name or f"tab_{i}", "url": tab_url})
        except Exception:
            continue

    return out


def unique_tab_key(base: str, used: set) -> str:
    base = clean_text(base) or "tab"
    if base not in used:
        used.add(base)
        return base
    i = 2
    while f"{base} ({i})" in used:
        i += 1
    k = f"{base} ({i})"
    used.add(k)
    return k


async def scrape_one(context, link_data: Dict[str, str]) -> Dict[str, Any]:
    page = await context.new_page()
    url = link_data["url"]
    try:
        await block_resources(page)

        await page.goto(url, timeout=NAV_TIMEOUT_MS, wait_until="domcontentloaded")
        try:
            await page.wait_for_selector("#contenido", timeout=5000)
        except Exception:
            pass

        result: Dict[str, Any] = {
            "url": url,
            "scraped_at": now_iso_utc(),
            # Añadimos datos del fichero de links para enriquecer el resultado
            "filtro_provincia": link_data.get("provincia"),
            "filtro_localidad": link_data.get("localidad"),
            "filtro_tipo_bien": link_data.get("tipo_bien"),
        }

        h = page.locator("#contenido h2, #contenido h1, h2, h1, h3").first
        if await h.count() > 0:
            t = clean_text((await h.text_content()) or "")
            if t:
                result["titulo"] = t

        tabs = await collect_tab_links(page)

        if not tabs:
            data = await extract_mixed_content(page)
            if data:
                result["Contenido"] = data
            return result

        used_keys = set(result.keys())

        for tab in tabs:
            tab_name = unique_tab_key(tab.get("name", ""), used_keys)
            tab_url = tab["url"]

            await page.goto(tab_url, timeout=NAV_TIMEOUT_MS, wait_until="domcontentloaded")
            try:
                await page.wait_for_selector("#contenido", timeout=5000)
            except Exception:
                pass

            data = await extract_mixed_content(page)
            result[tab_name] = data

        return result
    finally:
        await page.close()


# ---------------- QUEUE PIPELINE ----------------
async def worker(context, in_queue: asyncio.Queue, out_queue: asyncio.Queue):
    while True:
        try:
            link_data = in_queue.get_nowait()
        except asyncio.QueueEmpty:
            break

        last_err: Optional[str] = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                obj = await scrape_one(context, link_data)

                keys = set(obj.keys()) - {"url", "scraped_at", "titulo", "error"}
                if not keys:
                    raise Exception("No se detectaron secciones o datos válidos")

                await out_queue.put(obj)
                last_err = None
                break
            except PlaywrightTimeoutError as e:
                last_err = f"Timeout: {e}"
            except Exception as e:
                last_err = str(e)

            await asyncio.sleep(2 * attempt)

        if last_err:
            logger.error(f"FALLO FINAL en {link_data['url']}: {last_err}")
            await out_queue.put(
                {
                    "url": link_data['url'],
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

                count += 1
                if "error" in item:
                    logger.warning(f"[{count}/{total_pending}] ERROR guardado para {item['url']}")
                else:
                    titulo = item.get("titulo", "Sin título")
                    logger.info(f"[{count}/{total_pending}] OK | {titulo[:60]}")
            finally:
                out_queue.task_done()


async def main():
    links = load_links_data(INPUT_LINKS_JSONL)
    if not links:
        logger.error(f"No se encontraron URLs en {INPUT_LINKS_JSONL}")
        return

    processed = load_processed_urls(OUTPUT_DETAIL_JSONL)
    # Filtramos los objetos de link, no solo las URLs
    pending_links = [link for link in links if link.get("url") not in processed]

    test_limit = TEST_LIMIT
    if test_limit and len(pending_links) > test_limit:
        logger.info(f"MODO PRUEBA ACTIVADO: Procesando solo {test_limit} de {len(pending_links)} URLs.")
        pending_links = pending_links[:test_limit]

    logger.info(
        f"URLs totales en el fichero de entrada: {len(links)} | Ya procesadas: {len(processed)} | Pendientes: {len(pending_links)}"
    )
    if not pending_links:
        logger.info("No hay nada pendiente. Listo.")
        return

    in_queue: asyncio.Queue = asyncio.Queue()
    for link in pending_links:
        in_queue.put_nowait(link)

    out_queue: asyncio.Queue = asyncio.Queue()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=HEADLESS)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
        )

        writer_task = asyncio.create_task(writer(out_queue, OUTPUT_DETAIL_JSONL, len(pending_links)))
        workers = [asyncio.create_task(worker(context, in_queue, out_queue)) for _ in range(MAX_CONCURRENCY)]

        await asyncio.gather(*workers)

        await out_queue.put(None)
        await writer_task

        await browser.close()

    logger.info("Proceso completado.")


if __name__ == "__main__":
    asyncio.run(main())