import os
import sys
import time
import random
import logging
import requests
from requests.adapters import HTTPAdapter
import concurrent.futures
import argparse
import json
from datetime import datetime
from threading import Lock
from typing import List, Dict, Any, Optional, Union, Tuple
from dotenv import load_dotenv

# Load environment variables
# Look for .env-scripts in the project root (one directory up)
dotenv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env-scripts')
load_dotenv(dotenv_path)

# --- CONFIGURATION ---
PROXY_LIST_URL: str = "https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/http/data.txt"

USER_AGENTS: List[str] = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 14; Pixel 8 Pro) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
]

# --- LOGGING ---

def setup_logging(level_name: str = "INFO") -> None:
    """Sets up basic logging configuration."""
    level = getattr(logging, level_name.upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format='%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    # Silence third-party libs
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)

def status(step: str, message: str) -> None:
    """Legacy wrapper for logging to maintain compatibility during refactor."""
    logging.info(f"[{step}] {message}")


# --- API CLIENT ---
API_BASE_URL: str = os.getenv("API_BASE_URL", "http://p2capi:8080/api") # Internal Docker URL
API_KEY: Optional[str] = os.getenv("API_KEY")

# Global session with connection pooling for APIClient
_global_api_session = requests.Session()
_global_api_adapter = HTTPAdapter(pool_connections=200, pool_maxsize=200, max_retries=3)
_global_api_session.mount('http://', _global_api_adapter)
_global_api_session.mount('https://', _global_api_adapter)

class APIClient:
    def __init__(self) -> None:
        self.base_url: str = API_BASE_URL
        self.headers: Dict[str, Optional[str]] = {
            "X-API-KEY": API_KEY,
            "Content-Type": "application/json"
        }

    def _request(self, method: str, endpoint: str, **kwargs: Any) -> Any:
        """Internal helper to handle requests, error logging, and JSON parsing."""
        url = f"{self.base_url}/{endpoint}"
        try:
            resp = _global_api_session.request(method, url, headers=self.headers, timeout=60, **kwargs)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            status("API", f"{method} Failed: {url} | {e}")
            if hasattr(e, 'response') and e.response is not None:
                status("API", f"Response: {e.response.text}")
            raise

    def post_ingestion(self, endpoint: str, data: Any) -> Any:
        """Helper for ingestion endpoints (adds 'ingestion/' prefix)."""
        return self._request("POST", f"ingestion/{endpoint}", json=data)

    def get(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """Performs a GET request to the specified endpoint."""
        return self._request("GET", endpoint, params=params)

    def post(self, endpoint: str, data: Any) -> Any:
        """Performs a POST request to the specified endpoint."""
        return self._request("POST", endpoint, json=data)

# --- PROXY ---
def check_proxy(proxy: str, test_url: str = "http://example.com", timeout: int = 5) -> Optional[str]:
    """Tests a single proxy against a reliable target."""
    proxies_dict = {"http": f"http://{proxy}", "https": f"http://{proxy}"}
    try:
        resp = requests.get(test_url, proxies=proxies_dict, timeout=timeout, verify=False)
        resp.raise_for_status()
        return proxy
    except Exception:
        return None

def validate_proxies(proxies_list: List[str], batch_size: int = 50, target_count: Optional[int] = None, test_url: str = "http://example.com") -> List[str]:
    """
    Validates a list of proxies in parallel to find working ones.
    If target_count is set, it returns as soon as enough proxies are found.
    """
    random.shuffle(proxies_list)
    
    # If injected by Orchestrator, assume they are already validated
    if os.environ.get("ORCHESTRATOR_VALIDATED") == "1":
        status("ProxyManager", f"Using {len(proxies_list)} pre-validated proxies from Orchestrator.")
        return proxies_list

    valid_proxies: List[str] = []
    status("ProxyManager", f"Validating {len(proxies_list)} proxies against {test_url}...")
    
    for i in range(0, len(proxies_list), batch_size):
        batch = proxies_list[i:i + batch_size]
        with concurrent.futures.ThreadPoolExecutor(max_workers=batch_size) as executor:
            # check_proxy defaults to example.com, suitable for general connectivity
            futures = [executor.submit(check_proxy, proxy, test_url) for proxy in batch]
            results = [f.result() for f in futures]
        
        valid_batch = [proxy for proxy in results if proxy]
        valid_proxies.extend(valid_batch)
        
        status("ProxyManager", f"Batch {i//batch_size + 1}: Found {len(valid_batch)} working proxies. Total valid: {len(valid_proxies)}")
        
        if target_count and len(valid_proxies) >= target_count:
            break
            
    return valid_proxies

def get_proxies_from_source(source_url: str = PROXY_LIST_URL, config: Optional[Dict[str, Any]] = None) -> List[str]:
    """
    Fetches raw proxies from the source URL.
    Prioritizes:
    1. config['proxies'] (list of strings)
    2. ORCHESTRATOR_PROXIES env var
    3. Source URL download
    """
    # 1. Config Injection
    if config and "proxies" in config:
        status("ProxyManager", f"Using {len(config['proxies'])} proxies from Config.")
        return config["proxies"]

    # 2. Env Var Injection
    injected_proxies = os.environ.get("ORCHESTRATOR_PROXIES")
    if injected_proxies:
        status("ProxyManager", "Loading proxies from ORCHESTRATOR_PROXIES env var.")
        return [p.strip() for p in injected_proxies.split(',') if p.strip()]

    # 3. Orchestrator API Injection
    orchestrator_url = os.environ.get("ORCHESTRATOR_API_URL")
    if orchestrator_url:
        try:
            url = f"{orchestrator_url}/api/proxies/list"
            status("ProxyManager", f"Fetching validated proxies from Orchestrator API: {url}...")
            resp = requests.get(url, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                proxies = data.get("proxies", [])
                if proxies:
                    status("ProxyManager", f"Loaded {len(proxies)} validated proxies from Orchestrator.")
                    os.environ["ORCHESTRATOR_VALIDATED"] = "1" # Hint to skip local validation
                    return proxies
        except Exception as e:
            status("ProxyManager", f"Failed to fetch from Orchestrator API: {e}")

    try:
        status("ProxyManager", f"Fetching proxies from {source_url}...")
        proxy_resp = requests.get(source_url, timeout=10)
        proxy_resp.raise_for_status()
        proxies_list = [
            line.split("://")[-1].strip() 
            for line in proxy_resp.text.splitlines() 
            if line.strip() and not line.startswith('#')
        ]
        return proxies_list
    except Exception as e:
        status("ProxyManager", f"Failed to fetch proxy list: {e}")
        return []


def get_resilient_session(user_agent, proxy_pool, verify=True, test_url=None):
    """
    Acquires a new, fresh requests.Session object.
    Implements the 3x3 retry strategy:
    - Try up to 3 distinct proxies.
    - 3 attempts per proxy.
    - If no proxy_pool, tries direct connection 3 times.
    
    Returns: (session, proxy_used)
    """
    headers = {"User-Agent": user_agent or random.choice(USER_AGENTS)}

    # Direct Mode (No Proxies)
    if not proxy_pool:
        for attempt in range(3):
            try:
                session = requests.Session()
                # Use a specific URL to test if provided, else just return the session
                # If test_url is provided, we actually make a request to verify connectivity
                if test_url:
                     resp = session.get(test_url, headers=headers, timeout=20, verify=verify)
                     resp.raise_for_status()
                
                return session, None
            except Exception as e:
                logging.warning(f"Direct connection attempt {attempt+1}/3 failed: {e}")
                time.sleep(1)
        return None, None

    # Proxy Mode
    # Copy and shuffle to avoid modifying original list in place if caller cares
    local_proxy_pool = list(proxy_pool)
    random.shuffle(local_proxy_pool)
    
    # Try up to 3 distinct proxies
    for proxy_idx, proxy in enumerate(local_proxy_pool[:3]):
        proxies_dict = {"http": f"http://{proxy}", "https": f"http://{proxy}"}
        
        # Try 3 times per proxy
        for attempt in range(3):
            try:
                session = requests.Session()
                # If test_url provided, verify connection
                if test_url:
                    resp = session.get(test_url, headers=headers, proxies=proxies_dict, timeout=20, verify=verify)
                    resp.raise_for_status()
                
                return session, proxy 
            except requests.RequestException as e:
                logging.warning(f"Proxy {proxy} attempt {attempt+1}/3 failed: {e}")
                time.sleep(1)
        
        logging.warning(f"Proxy {proxy} failed 3 times. Switching...")

    return None, None

def refresh_proxy_pool(current_pool: List[str]) -> List[str]:
    """
    Attempts to refresh the proxy pool from the Orchestrator API.
    Updates the provided list in-place if successful, and returns the new list.
    """
    api_url = os.getenv("ORCHESTRATOR_API_URL")
    if not api_url:
        logging.info("[ProxyRefresh] No ORCHESTRATOR_API_URL set. Cannot refresh.")
        return current_pool

    try:
        url = f"{api_url}/api/proxies/list"
        logging.info(f"[ProxyRefresh] Fetching fresh proxies from {url}...")
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        new_proxies = data.get("proxies", [])
        
        if new_proxies:
            logging.info(f"[ProxyRefresh] Refreshed pool with {len(new_proxies)} proxies.")
            # Update in place
            current_pool[:] = new_proxies
            return current_pool
        else:
            logging.warning("[ProxyRefresh] API returned 0 proxies.")
            return current_pool
            
    except Exception as e:
        status("ProxyRefresh", f"Failed to refresh proxies: {e}")
        return current_pool


def get_session(proxy_pool: Optional[List[str]] = None, user_agent: Optional[str] = None) -> Tuple[requests.Session, Optional[str]]:
    """
    Returns a requests.Session. 
    If proxy_pool is provided, assigns a random proxy.
    """
    session = requests.Session()
    
    ua = user_agent if user_agent else random.choice(USER_AGENTS)
    session.headers.update({
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive"
    })
    
    proxy: Optional[str] = None
    if proxy_pool:
        proxy = random.choice(proxy_pool)
        session.proxies = {"http": f"http://{proxy}", "https": f"http://{proxy}"}
    
    return session, proxy

# --- DATE PARSING ---
def parse_date(date_str: Optional[str], formats: Optional[List[str]] = None) -> Optional[datetime]:
    """
    Tries to parse a date string using a list of formats.
    Default formats: '%m/%d/%Y %I:%M:%S %p', '%m/%d/%Y', '%Y%m%d', ISO format
    """
    if not date_str: return None
    
    # Handle ISO format with Z
    if 'Z' in date_str:
        try:
            return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        except ValueError:
            pass

    if formats is None:
        formats = [
            '%m/%d/%Y %I:%M:%S %p', # 9/20/2025 12:00:00 AM
            '%m/%d/%Y',             # 9/20/2025
            '%Y%m%d',               # 20250920
            '%Y-%m-%d'              # 2025-09-20
        ]
        
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
            
    return None

# --- RETRY LOGIC (Tenacity) ---
try:
    from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type, before_sleep_log
    
    def get_retry_decorator(max_attempts=3, wait_seconds=2):
        """
        Returns a tenacity retry decorator.
        Retries on requests.RequestException and general Exception.
        """
        return retry(
            stop=stop_after_attempt(max_attempts),
            wait=wait_fixed(wait_seconds),
            retry=retry_if_exception_type((requests.RequestException, Exception)),
            before_sleep=before_sleep_log(logging.getLogger(), logging.WARNING)
        )
except ImportError:
    # Fallback if tenacity isn't installed (though it should be)
    def get_retry_decorator(max_attempts=3, wait_seconds=2):
        def decorator(func):
            return func
        return decorator

def get_config() -> Dict[str, Any]:
    """
    Parses optional --config JSON argument and returns a dictionary.
    Falls back to empty dict if not provided.
    """
    parser = argparse.ArgumentParser(add_help=False) # Partial parser
    parser.add_argument("--config", type=str, default="{}")
    args, unknown = parser.parse_known_args()
    try:
        return json.loads(args.config)
    except Exception as e:
        status("Config", f"Failed to parse config JSON: {e}")
        return {}
