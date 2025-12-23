from __future__ import annotations

import atexit
import subprocess
import tempfile
from enum import Enum
from pathlib import Path
from typing import Union, Optional

import requests
import urllib3
from requests.adapters import HTTPAdapter, Retry
from tqdm import tqdm

# Optional: System Certificate Patching for Corporate SSL Inspection
try:
    import pip_system_certs.wrappers
except ImportError:
    pass

# Optional: SSPI Authentication (NTLM/Kerberos)
try:
    from requests_negotiate_sspi import HttpNegotiateAuth
except ImportError:
    HttpNegotiateAuth = None

from .config import Config
from .logger import setup_logger

_logger = setup_logger()


class DownloaderType(Enum):
    POWERSHELL = "powershell"
    PYTHON = "python"

    @classmethod
    def has_value(cls, value: str) -> bool:
        return value and value.lower() in (item.value for item in cls)


class Downloader:
    """
    Robust Downloader with SSPI Session Rotation and Auto-Recovery.
    """

    def __init__(self, config: Config) -> None:
        self.config = config
        
        dtype = getattr(self.config, "downloader", "python").lower()
        if not DownloaderType.has_value(dtype):
            raise ValueError(f"Invalid downloader '{dtype}'")
        self.backend = DownloaderType(dtype)

        # Network Configuration
        self.use_sspi = getattr(self.config, "use_sspi", True)
        self.proxy_url = getattr(self.config, "proxy_url", None)
        self.verify_ssl = getattr(self.config, "verify_ssl", True)
        self.ca_bundle = getattr(self.config, "ca_bundle", None)
        self.retries = getattr(self.config, "retries", 3)
        
        # Timeouts (Connect, Read)
        self.timeout = (
            getattr(self.config, "timeout_connect", 10),
            getattr(self.config, "timeout_read", 60)
        )

        self.session: Optional[requests.Session] = None

        if self.backend == DownloaderType.PYTHON:
            self._init_session()
            atexit.register(self.close)

    def _init_session(self) -> None:
        """
        Initializes (or Hard-Resets) the requests Session.
        This generates a fresh NTLM/Kerberos ticket and clears broken socket pools.
        """
        if self.session:
            try:
                self.session.close()
            except Exception:
                pass

        self.session = requests.Session()

        # 1. Force Clean Environment
        # Critical: Ignore Windows Env Vars to prevent conflict with our explicit Config
        self.session.trust_env = False 

        # 2. Proxy Setup
        if self.proxy_url:
            self.session.proxies.update({
                "http": self.proxy_url, 
                "https": self.proxy_url
            })

        # 3. Connection Resilience (Keep-Alive + Retries)
        # We mount this even in 'Smart' mode because the TCP pool helps 
        # when downloading many small repodata chunks sequentially.
        retry_strategy = Retry(
            total=self.retries,
            backoff_factor=0.3,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"]
        )
        adapter = HTTPAdapter(pool_connections=10, pool_maxsize=10, max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        # 4. SSL Logic
        if not self.verify_ssl:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            self.session.verify = False
            _logger.warning("SSL verification disabled (Insecure).")
        else:
            self.session.verify = self.ca_bundle if self.ca_bundle else True

        # 5. SSPI Authentication
        if self.use_sspi:
            if HttpNegotiateAuth:
                _logger.debug("Enabling SSPI (Negotiate/Kerberos) authentication")
                self.session.auth = HttpNegotiateAuth()
            else:
                _logger.warning("SSPI requested but 'requests-negotiate-sspi' not found.")

    def close(self) -> None:
        if self.session:
            self.session.close()

    # --------------------------------------------------------
    # Download Methods
    # --------------------------------------------------------

    def download_to_file(self, url: str, output_path: Union[str, Path]) -> None:
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        if self.backend == DownloaderType.PYTHON:
            self._download_python_smart(url, output_path)
        else:
            self._download_powershell(url, output_path)

    def _download_python_smart(self, url: str, output_path: Path) -> None:
        """
        Smart Download with Auto-Recovery.
        - Attempt 1: Try using existing connection.
        - Failure (407/Socket Closed): Wipe session, Re-Auth, Retry.
        """
        # Hard limit on internal retries (distinct from HTTP adapter retries)
        # This covers logic errors (Auth) rather than network hiccups.
        max_logic_retries = 2
        
        for attempt in range(1, max_logic_retries + 1):
            try:
                if not self.session:
                    self._init_session()

                # HYBRID STREAMING HEURISTIC:
                # RPMs are large -> Stream to save RAM.
                # XML/SQLite are medium/small -> No Stream to force immediate 401 handshake completion.
                # (You can tune this heuristic based on your actual repo data sizes)
                is_large_file = str(output_path).endswith(".rpm")
                use_stream = is_large_file

                _logger.debug("Downloading %s (Attempt %d, stream=%s)", url, attempt, use_stream)

                with self.session.get(
                    url, 
                    stream=use_stream, 
                    timeout=self.timeout
                ) as resp:
                    
                    # TRIGGER RETRY: Proxy said "Ticket Invalid" or "Login Required"
                    if resp.status_code == 407:
                        raise requests.exceptions.ProxyError("407 Proxy Auth Required - Session Expired")
                    
                    resp.raise_for_status()
                    
                    total = int(resp.headers.get("content-length", 0) or 0)
                    
                    with open(output_path, "wb") as fh:
                        if use_stream:
                            with tqdm(total=total, unit="B", unit_scale=True, desc=output_path.name) as bar:
                                for chunk in resp.iter_content(chunk_size=8192):
                                    if chunk:
                                        fh.write(chunk)
                                        bar.update(len(chunk))
                        else:
                            # Direct write for non-streamed (forces full read into memory buffer first)
                            fh.write(resp.content)
                            
                _logger.debug("Downloaded %s -> %s", url, output_path)
                return

            except (requests.exceptions.ProxyError, 
                    requests.exceptions.ChunkedEncodingError, 
                    requests.exceptions.ConnectionError, 
                    requests.exceptions.SSLError) as e:
                
                # AUTO-RECOVERY BLOCK
                if attempt < max_logic_retries:
                    _logger.warning("Connection rejected (%s). Renewing NTLM Session and Retrying...", e)
                    self._init_session() # <--- THE FIX: New Session = New Ticket
                else:
                    _logger.error("Failed to download %s after %d attempts.", url, max_logic_retries)
                    raise
            except Exception as e:
                _logger.error("Unexpected error for %s: %s", url, e)
                raise

    def download_to_memory(self, url: str, max_size_mb: int = 100) -> bytes:
        """
        Download to bytes with Session Rotation logic.
        Useful for repomd.xml and small metadata.
        """
        if self.backend != DownloaderType.PYTHON:
            return self._download_powershell_memory_fallback(url)

        max_logic_retries = 2
        for attempt in range(1, max_logic_retries + 1):
            try:
                if not self.session:
                    self._init_session()
                
                # For memory downloads, we usually want metadata.
                # We use stream=False to ensure the Auth handshake completes fully 
                # before we try to parse anything.
                _logger.debug("Downloading %s to memory (Attempt %d)", url, attempt)
                
                # We use stream=True initially ONLY to check Content-Length headers for safety
                with self.session.get(url, stream=True, timeout=self.timeout) as resp:
                    if resp.status_code == 407:
                         raise requests.exceptions.ProxyError("407 Proxy Auth Required")
                    
                    resp.raise_for_status()
                    
                    content_len = int(resp.headers.get("content-length", 0))
                    if content_len > (max_size_mb * 1024 * 1024):
                        raise ValueError(f"File too large ({content_len} bytes) for memory download")
                    
                    # Safe to consume
                    return resp.content

            except (requests.exceptions.ProxyError, requests.exceptions.ConnectionError, requests.exceptions.SSLError) as e:
                if attempt < max_logic_retries:
                    _logger.warning("Memory download failed (%s). Renewing Session...", e)
                    self._init_session()
                else:
                    raise

        raise RuntimeError("Unreachable")

    # --------------------------------------------------------
    # Powershell Legacy Backends
    # --------------------------------------------------------

    def _download_powershell(self, url: str, output_path: Path) -> None:
        # Using Invoke-WebRequest (IWR) as it is modern compared to WebClient
        ps_cmd = (
            f"$ProgressPreference = 'SilentlyContinue'; " # Suppress PS progress bar that clutters stdout
            f"[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12; "
            f"Invoke-WebRequest -Uri '{url}' -OutFile '{str(output_path)}' -UseDefaultCredentials"
        )
        
        if self.proxy_url:
            ps_cmd += f" -Proxy '{self.proxy_url}'"

        try:
            subprocess.run(["powershell", "-NoProfile", "-Command", ps_cmd], check=True, capture_output=True)
            _logger.debug("Downloaded %s (PowerShell)", url)
        except subprocess.CalledProcessError as e:
            _logger.error("PowerShell download failed: %s", e.stderr.decode('utf-8', errors='ignore'))
            raise RuntimeError(f"PowerShell download failed for {url}")

    def _download_powershell_memory_fallback(self, url: str) -> bytes:
        tf = tempfile.NamedTemporaryFile(delete=False)
        tf.close()
        try:
            self._download_powershell(url, Path(tf.name))
            with open(tf.name, "rb") as fh:
                return fh.read()
        finally:
            Path(tf.name).unlink(missing_ok=True)