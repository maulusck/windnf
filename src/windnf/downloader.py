# downloader.py
from __future__ import annotations

import logging
import subprocess
import tempfile
from enum import Enum
from pathlib import Path
from typing import Any, Optional, Union

import requests
from requests.adapters import HTTPAdapter, Retry
from tqdm import tqdm

from .config import Config
from .logger import setup_logger

_logger = setup_logger()
logger = _logger


# Minimal enum to select backend
class DownloaderType(Enum):
    POWERSHELL = "powershell"
    PYTHON = "python"

    @classmethod
    def has_value(cls, value: str) -> bool:
        return value and value.lower() in (item.value for item in cls)


class Downloader:
    """
    Downloader abstraction:
      - built from Config instance
      - supports download_to_memory(url) -> bytes
      - supports download_to_file(url, path)
      - Python backend uses requests + retries
      - PowerShell backend uses powershell invocation (Windows), writes to temp file and reads
    """

    def __init__(self, config: Config) -> None:
        self.config = config

        downloader_type = getattr(self.config, "downloader", "python").lower()
        if not DownloaderType.has_value(downloader_type):
            raise ValueError(f"Invalid downloader '{downloader_type}'")
        self.backend = DownloaderType(downloader_type)

        proxy_url = getattr(self.config, "proxy_url", None)
        skip_ssl_verify = getattr(self.config, "skip_ssl_verify", True)

        if self.backend == DownloaderType.PYTHON:
            # Create a resilient requests.Session
            self.session = requests.Session()
            if proxy_url:
                self.session.proxies.update({"http": proxy_url, "https": proxy_url})
            else:
                self.session.trust_env = True
            self.session.verify = not skip_ssl_verify
            retries = Retry(total=3, backoff_factor=0.3, status_forcelist=(500, 502, 503, 504))
            adapter = HTTPAdapter(max_retries=retries)
            self.session.mount("http://", adapter)
            self.session.mount("https://", adapter)
        else:
            self.session = None

    # -------------------------
    # file-based download
    # -------------------------
    def download_to_file(self, url: str, output_path: Union[str, Path]) -> None:
        """
        Download resource to file path (output_path). Uses selected backend.
        """
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        if self.backend == DownloaderType.PYTHON:
            self._download_python_to_file(url, output_path)
        else:
            self._download_powershell_to_file(url, output_path)

    def _download_python_to_file(self, url: str, output_path: Path) -> None:
        if not self.session:
            raise RuntimeError("Python downloader not initialized")
        try:
            with self.session.get(url, stream=True, timeout=60) as resp:
                resp.raise_for_status()
                total = int(resp.headers.get("content-length", 0) or 0)
                with open(output_path, "wb") as fh, tqdm(
                    total=total, unit="iB", unit_scale=True, desc=output_path.name
                ) as bar:
                    for chunk in resp.iter_content(chunk_size=8192):
                        if chunk:
                            fh.write(chunk)
                            bar.update(len(chunk))
            logger.debug("Downloaded %s -> %s (python)", url, output_path)
        except Exception as e:
            logger.exception("Python downloader failed for %s: %s", url, e)
            raise

    def _download_powershell_to_file(self, url: str, output_path: Path) -> None:
        # Powershell WebClient.DownloadFile — non-streaming
        ps_script = (
            f"$wc = New-Object System.Net.WebClient; "
            f"$wc.Proxy.Credentials = [System.Net.CredentialCache]::DefaultNetworkCredentials; "
            f"$wc.DownloadFile('{url}', '{str(output_path)}');"
        )
        result = subprocess.run(["powershell", "-NoProfile", "-Command", ps_script], capture_output=True, text=True)
        if result.returncode != 0:
            logger.error("PowerShell download failed: %s", result.stderr.strip())
            raise RuntimeError(f"PowerShell download failed: {result.stderr.strip()}")
        logger.debug("Downloaded %s -> %s (powershell)", url, output_path)

    # -------------------------
    # memory-based download
    # -------------------------
    def download_to_memory(self, url: str) -> bytes:
        """
        Download the URL and return bytes.
        - Python backend streams into memory (efficient for moderate sizes).
        - PowerShell backend downloads to a temp file and reads into memory.
        """
        if self.backend == DownloaderType.PYTHON:
            if not self.session:
                raise RuntimeError("Python downloader not initialized")
            try:
                with self.session.get(url, stream=True, timeout=60) as resp:
                    resp.raise_for_status()
                    parts = []
                    for chunk in resp.iter_content(chunk_size=8192):
                        if chunk:
                            parts.append(chunk)
                    data = b"".join(parts)
                    logger.debug("Downloaded %d bytes from %s (python)", len(data), url)
                    return data
            except Exception as e:
                logger.exception("download_to_memory failed for %s: %s", url, e)
                raise
        else:
            # Powershell: download to temporary file then read bytes
            tf = tempfile.NamedTemporaryFile(delete=False)
            tf.close()
            try:
                self._download_powershell_to_file(url, Path(tf.name))
                with open(tf.name, "rb") as fh:
                    data = fh.read()
                logger.debug("Downloaded %d bytes from %s (powershell via temp file)", len(data), url)
                return data
            finally:
                try:
                    Path(tf.name).unlink()
                except Exception:
                    pass
