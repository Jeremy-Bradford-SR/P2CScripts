import threading
import time
import os
import requests
import random
import re
import logging
from concurrent.futures import ThreadPoolExecutor
import json
from typing import List, Dict, Set, Optional, Any, Union

# Configure logger for this module
logger = logging.getLogger(__name__)

class ProxyManager:
    _instance: Optional['ProxyManager'] = None
    _lock: threading.RLock = threading.RLock()
    
    def __new__(cls) -> 'ProxyManager':
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(ProxyManager, cls).__new__(cls)
                    # Config
                    cls._instance.config = {
                        "concurrency": 250, 
                        "ttl": 600, 
                        "test_url": "http://p2c.cityofdubuque.org/main.aspx",
                        "target_pool_size": 100,
                        "sources": [
                            "https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/http/data.txt"
                        ]
                    }
                    
                    # Try to load from DB explicitly on boot
                    cls._instance._load_config_from_db()
                    
                    # State
                    cls._instance.valid_proxies = [] # type: List[str]
                    cls._instance.raw_proxies_pool = set() # type: Set[str]
                    cls._instance.proxy_failures = {} # type: Dict[str, int]  # Track failures for LRU eviction
                    cls._instance.total_raw = 0
                    cls._instance.running = False
                    cls._instance.last_fetch_time = 0.0
                    cls._instance.churn_stats = {"checked": 0, "success": 0}

        return cls._instance

    def _load_config_from_db(self) -> None:
        try:
            from .db import get_db_connection, return_db_connection
            conn = get_db_connection()
            if conn:
                cursor = conn.cursor()
                cursor.execute("SELECT config_value FROM orchestrator_config WHERE config_key = 'proxy_manager_config'")
                row = cursor.fetchone()
                if row:
                    new_config = json.loads(row[0])
                    # Update config safely
                    if hasattr(self, '_lock'):
                        with self._lock:
                            self.config.update(new_config)
                    else:
                        self.config.update(new_config)
                return_db_connection(conn)
        except Exception as e:
            logger.error(f"[ProxyManager] Error loading config from DB: {e}")

    def start_refresher(self) -> None:
        if self.running: return
        self.running = True
        threading.Thread(target=self._fetch_loop, daemon=True).start()
        threading.Thread(target=self._churn_loop, daemon=True).start()

    def get_status(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "active_proxies": len(self.valid_proxies),
                "total_raw": len(self.raw_proxies_pool),
                "is_active": self.running,
                "churn_stats": self.churn_stats,
                "config": self.config
            }

    def update_config(self, new_config: Dict[str, Any]) -> None:
        with self._lock:
            self.config.update(new_config)

    def force_refresh(self) -> bool:
        threading.Thread(target=self._fetch_sources, daemon=True).start()
        return True

    def get_proxies(self) -> List[str]:
        with self._lock:
            return list(self.valid_proxies)

    def _fetch_loop(self) -> None:
        logger.info("[ProxyManager] Started Source Fetch Loop.")
        while self.running:
            try:
                # Dynamically reload Config from Database periodically
                self._load_config_from_db()

                # Re-fetch sources if TTL expired or pool is empty
                if not self.raw_proxies_pool or (time.time() - self.last_fetch_time > self.config["ttl"]):
                    self._fetch_sources()
            except Exception as e:
                logger.error(f"[ProxyManager] Error in fetch loop: {e}")
            time.sleep(60) 

    def _churn_loop(self) -> None:
        logger.info("[ProxyManager] Started Churn Validation Loop.")
        while self.running:
            try:
                # Check 1: Do we need more proxies?
                target_size = self.config.get("target_pool_size", 50)
                if len(self.valid_proxies) >= target_size:
                    time.sleep(5) # Pause churn if pool is full
                    continue

                if not self.raw_proxies_pool:
                    time.sleep(5)
                    continue

                # 1. Pick Batch
                concurrency = self.config["concurrency"]
                with self._lock:
                    pool_list = list(self.raw_proxies_pool)
                 
                # Random sample
                batch_size = min(len(pool_list), concurrency)
                to_check = random.sample(pool_list, batch_size)
                
                # 2. Validate Batch
                test_url = self.config["test_url"]
                working_batch: List[str] = []
                
                with ThreadPoolExecutor(max_workers=concurrency) as executor:
                    futures = [executor.submit(self._check_proxy, p, test_url) for p in to_check]
                    for f in futures:
                        res = f.result()
                        if res: working_batch.append(res)
                
                # 3. Update State
                with self._lock:
                    current_valid_set = set(self.valid_proxies)
                    
                    # Add successes
                    for p in working_batch:
                        current_valid_set.add(p)
                        # Reset failure count on success
                        self.proxy_failures.pop(p, None)
                        
                    # Track failures and evict from raw pool after 3 strikes
                    successful_set = set(working_batch)
                    for p in to_check:
                        if p not in successful_set:
                            # Track failure
                            self.proxy_failures[p] = self.proxy_failures.get(p, 0) + 1
                            
                            # Remove from valid pool
                            if p in current_valid_set:
                                current_valid_set.remove(p)
                            
                            # LRU Eviction: Remove from raw pool after 3 failures
                            if self.proxy_failures[p] >= 3:
                                self.raw_proxies_pool.discard(p)
                                self.proxy_failures.pop(p, None)  # Clean up tracking
                             
                    self.valid_proxies = list(current_valid_set)
                    
                    # Update stats
                    self.churn_stats["checked"] += len(to_check)
                    self.churn_stats["success"] += len(working_batch)

                # Sleep slightly
                time.sleep(1)

            except Exception as e:
                logger.error(f"[ProxyManager] Error in churn loop: {e}")
                time.sleep(5)

    def _fetch_sources(self) -> None:
        logger.info("[ProxyManager] Fetching sources...")
        new_pool: Set[str] = set()
        sources = self.config["sources"]
        
        for url in sources:
            try:
                resp = requests.get(url.strip(), timeout=10)
                if resp.status_code == 200:
                    parsed = self._parse_proxies(resp.text)
                    new_pool.update(parsed)
            except Exception:
                pass # Silent fail per source
        
        with self._lock:
            # We Merge, not replace, to avoid losing manually added ones if we supported that.
            self.raw_proxies_pool.update(new_pool) 
            self.last_fetch_time = time.time()
            self.total_raw = len(self.raw_proxies_pool)
            
        logger.info(f"[ProxyManager] Sources Fetched. Total Raw Pool: {self.total_raw}")


    def _parse_proxies(self, text: str) -> Set[str]:
        found: Set[str] = set()
        # Regex for IP:PORT
        regex = r"(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}):(\d+)"
        
        for line in text.splitlines():
            line = line.strip()
            if not line or line.startswith("#") or line.startswith("Warning") or "IP" in line: continue
            
            # Match IP:Port
            match = re.search(regex, line)
            if match:
                ip, port = match.groups()
                # We normalize to IP:PORT string
                found.add(f"{ip}:{port}")
        return found

    def _check_proxy(self, proxy: str, url: str) -> Optional[str]:
        # Strict validation: Timeout or connection error -> Fail (Effective Ban for this cycle)
        proxies = {"http": f"http://{proxy}", "https": f"http://{proxy}"}
        try:
            resp = requests.get(url, proxies=proxies, timeout=5, verify=False)
            resp.raise_for_status() # Ban on 400/500
            return proxy
        except Exception:
            return None
