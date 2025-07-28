import asyncio
import sys
import httpx
import logging
import validators
import json
import time
import ipaddress
import socket
from typing import List, Dict, Any
from urllib.parse import urlparse
from contextlib import asynccontextmanager
import uvicorn
from collections import Counter
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from playwright.async_api import async_playwright, Browser, TimeoutError as PlaywrightTimeoutError, Error as PlaywrightError
import base64

# --- Custom Exception & Windows Fix ---
class TooManyRedirectsError(Exception): pass
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# --- Lifespan Manager ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Handles startup and shutdown to manage a single browser instance."""
    logger.info("Application startup: Launching browser...")
    p = await async_playwright().start()
    browser = await p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-gpu"])
    app.state.browser = browser
    yield
    logger.info("Application shutdown: Closing browser...")
    await browser.close()
    await p.stop()

# --- App Initialization ---
app = FastAPI(lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "https://urljourney.netlify.app", "*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Configuration & Caches ---
AKAMAI_IP_RANGES = ["23.192.0.0/11", "104.64.0.0/10", "184.24.0.0/13"]
ip_cache = {}

# --- HELPER FUNCTIONS (Your logic, made fully asynchronous) ---
async def resolve_ip_async(url: str) -> str:
    """Uses your original resolve_ip logic but runs it in a non-blocking way."""
    hostname = urlparse(url).hostname
    if not hostname: return None
    if hostname in ip_cache: return ip_cache[hostname]
    try:
        ip = await asyncio.to_thread(socket.gethostbyname, hostname)
        ip_cache[hostname] = ip
        return ip
    except (socket.gaierror, TypeError):
        return None

def is_akamai_ip(ip: str) -> bool:
    """Your is_akamai_ip function, unchanged."""
    if not ip: return False
    try:
        addr = ipaddress.ip_address(ip)
        for cidr in AKAMAI_IP_RANGES:
            if addr in ipaddress.ip_network(cidr):
                return True
    except ValueError:
        pass
    return False

async def get_server_name_async(headers: dict, url: str) -> str:
    """Your detailed get_server_name function, now fully async."""
    headers = {k.lower(): v for k, v in headers.items()}
    hostname = urlparse(url).hostname
    if hostname and ("bmw" in hostname.lower() or "mini" in hostname.lower()):
        if "cache-control" in headers:
            return "Apache (AEM)"
    server_value = headers.get("server", "").lower()
    if server_value:
        if "akamai" in server_value or "ghost" in server_value: return "Akamai"
        if "apache" in server_value: return "Apache (AEM)"
        return server_value.capitalize()
    
    server_timing = headers.get("server-timing", "")
    has_akamai_cache = "cdn-cache; desc=HIT" in server_timing or "cdn-cache; desc=MISS" in server_timing
    has_akamai_request_id = "x-akamai-request-id" in headers
    ip = await resolve_ip_async(url) # Using the non-blocking version
    is_akamai = is_akamai_ip(ip)
    has_dispatcher = "x-dispatcher" in headers or "x-aem-instance" in headers
    has_aem_paths = any("/etc.clientlibs" in v for h, v in headers.items() if h in ["link", "baqend-tags"])

    if has_akamai_cache or has_akamai_request_id or (server_timing and is_akamai):
        if has_aem_paths or has_dispatcher: return "Apache (AEM)"
        return "Akamai"
    if has_dispatcher or has_aem_paths: return "Apache (AEM)"
    if is_akamai: return "Akamai"
    return "Unknown"

# --- Analysis Functions ---
async def analyze_with_httpx(url: str, start_time: float) -> Dict[str, Any]:
    redirect_chain = []
    current_url = url
    try:
        async with httpx.AsyncClient(timeout=15.0, verify=False) as client:
            for _ in range(20):
                response = await client.get(current_url, follow_redirects=False)
                server_name = await get_server_name_async(dict(response.headers), str(response.url))
                redirect_chain.append({"url": str(response.url), "status": response.status_code, "server": server_name, "timestamp": time.time() - start_time})

                if not response.is_redirect:
                    if 'meta http-equiv="refresh"' in response.text.lower():
                        return {"needs_playwright": True, "reason": "Meta refresh tag"}
                    return {"engine": "httpx", "originalURL": url, "finalURL": str(response.url), "redirectChain": redirect_chain, "totalTime": time.time() - start_time}
                
                current_url = response.headers.get('location')
                if not current_url:
                    raise ValueError("Redirect header missing location")
                if not current_url.startswith(('http://', 'https://')):
                    current_url = urlparse(current_url, scheme=response.url.scheme, netloc=response.url.netloc).geturl()
            raise TooManyRedirectsError("Exceeded 20 redirects.")
    except (httpx.RequestError, TooManyRedirectsError, ValueError) as e:
        return {"needs_playwright": True, "reason": f"httpx failed: {e.__class__.__name__}"}

async def analyze_with_playwright(browser: Browser, url: str, start_time: float) -> Dict[str, Any]:
    context = None; redirect_chain = []
    try:
        context = await browser.new_context(user_agent="Mozilla/5.0...")
        page = await context.new_page()
        await page.route("**/*", lambda route: route.abort() if route.request.resource_type in {"image", "stylesheet", "font"} else route.continue_())
        
        async def handle_response(response):
            if response.request.is_navigation_request():
                headers = await response.all_headers()
                server = await get_server_name_async(headers, response.url)
                # To prevent duplicates from client-side refreshes, check URL and status
                if not redirect_chain or redirect_chain[-1]['url'] != response.url or redirect_chain[-1]['status'] != response.status:
                    redirect_chain.append({"url": response.url, "status": response.status, "server": server, "timestamp": time.time() - start_time})

        page.on("response", handle_response)
        await page.goto(url, timeout=60000, wait_until="domcontentloaded")
        
        return {"engine": "playwright", "originalURL": url, "finalURL": page.url, "redirectChain": redirect_chain, "totalTime": time.time() - start_time}
    finally:
        if context: await context.close()

async def hybrid_url_analyzer(browser: Browser, url: str) -> Dict[str, Any]:
    start_time = time.time()
    try:
        httpx_result = await analyze_with_httpx(url, start_time)
        if httpx_result.get("needs_playwright"):
            logger.info(f"Escalating {url} to Playwright.")
            return await analyze_with_playwright(browser, url, start_time)
        return httpx_result
    except (TooManyRedirectsError, PlaywrightTimeoutError, PlaywrightError, Exception) as e:
        error_message = f"{e.__class__.__name__}: {str(e).splitlines()[0]}"
        logger.error(f"Error on {url}: {error_message}")
        return {"originalURL": url, "error": error_message, "redirectChain": []}

# --- WebSocket Endpoint ---
@app.websocket("/analyze")
async def analyze_urls_websocket(websocket: WebSocket):
    await websocket.accept()
    browser = websocket.app.state.browser
    semaphore = asyncio.Semaphore(5)
    async def limited_fetch(url, apply_delay):
        if apply_delay: await asyncio.sleep(0.5)
        result = await hybrid_url_analyzer(browser, url)
        if websocket.client_state.name == 'CONNECTED': await websocket.send_text(json.dumps(result))
    try:
        data = await websocket.receive_json()
        raw_urls = list(set(filter(None, data.get("urls", []))))
        validated_urls_map = {u_str: (f"https://{u_str}" if not u_str.startswith(('http://', 'https://')) else u_str) for u_str in raw_urls}
        hostnames = [urlparse(u).hostname for u in validated_urls_map.values()]
        hostname_counts = Counter(hostnames)
        high_volume_domains = {h for h, c in hostname_counts.items() if c >= 30}
        tasks = [asyncio.create_task(limited_fetch(val_url, urlparse(val_url).hostname in high_volume_domains)) for val_url in validated_urls_map.values()]
        await asyncio.gather(*tasks)
        if websocket.client_state.name == 'CONNECTED': await websocket.send_text(json.dumps({"done": True}))
    except WebSocketDisconnect: logger.info("Client disconnected.")
    except Exception as e: logger.error(f"WebSocket error: {e}", exc_info=True)

@app.get("/test")
async def test():
    return {"status": "OK", "message": "Service operational"}