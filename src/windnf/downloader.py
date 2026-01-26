# downloader.py
from __future__ import annotations

import subprocess
import sys
import tempfile
import threading
import time
from enum import Enum
from pathlib import Path
from typing import Union

import requests
import urllib3
from requests.adapters import HTTPAdapter, Retry
from tqdm import tqdm

from .config import Config
from .logger import is_dumb_terminal, setup_logger

_logger = setup_logger()


class DownloaderType(Enum):
    POWERSHELL = "powershell"
    PYTHON = "python"

    @classmethod
    def has_value(cls, value: str) -> bool:
        return value and value.lower() in (item.value for item in cls)


class Downloader:
    def __init__(self, config: Config) -> None:
        self.config = config

        downloader_type = getattr(self.config, "downloader", "python").lower()
        if not DownloaderType.has_value(downloader_type):
            raise ValueError(f"Invalid downloader '{downloader_type}'")
        self.backend = DownloaderType(downloader_type)

        proxy_url = getattr(self.config, "proxy_url", None)
        skip_ssl_verify = getattr(self.config, "skip_ssl_verify", True)

        if skip_ssl_verify:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            _logger.warning(
                "Warning: SSL verification is disabled. This may expose your application to security risks!"
            )

        if self.backend == DownloaderType.PYTHON:
            self.session = requests.Session()
            if proxy_url:
                self.session.proxies.update({"http": proxy_url, "https": proxy_url})
            else:
                self.session.trust_env = True

            self.session.verify = not skip_ssl_verify
            retries = Retry(
                total=3,
                backoff_factor=0.3,
                status_forcelist=(500, 502, 503, 504),
            )
            adapter = HTTPAdapter(max_retries=retries)
            self.session.mount("http://", adapter)
            self.session.mount("https://", adapter)
        else:
            self.session = None

    # -------------------------
    # public API
    # -------------------------
    def download_to_file(self, url: str, output_path: Union[str, Path]) -> None:
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        if output_path.exists():
            _logger.info("File already downloaded: %s", output_path)
            return

        if self.backend == DownloaderType.PYTHON:
            self._download_python_to_file(url, output_path)
        else:
            self._download_powershell_to_file(url, output_path)

    def download_to_memory(self, url: str) -> bytes:
        if self.backend == DownloaderType.PYTHON:
            return self._download_python_to_memory(url)

        tf = tempfile.NamedTemporaryFile(delete=False)
        tf.close()
        try:
            self._download_powershell_to_file(url, Path(tf.name))
            return Path(tf.name).read_bytes()
        finally:
            try:
                Path(tf.name).unlink()
            except Exception:
                pass

    # -------------------------
    # python backend
    # -------------------------
    def _download_python_to_file(self, url: str, output_path: Path) -> None:
        if not self.session:
            raise RuntimeError("Python downloader not initialized")

        with self.session.get(url, stream=True, timeout=60) as resp:
            resp.raise_for_status()
            total = int(resp.headers.get("content-length", 0) or 0)

            with open(output_path, "wb") as fh, tqdm(
                total=total,
                unit="iB",
                unit_scale=True,
                desc=output_path.name,
                disable=is_dumb_terminal(),
            ) as bar:
                for chunk in resp.iter_content(chunk_size=8192):
                    if chunk:
                        fh.write(chunk)
                        bar.update(len(chunk))

        _logger.debug("Downloaded %s -> %s (python)", url, output_path)

    def _download_python_to_memory(self, url: str) -> bytes:
        if not self.session:
            raise RuntimeError("Python downloader not initialized")

        with self.session.get(url, stream=True, timeout=60) as resp:
            resp.raise_for_status()
            return b"".join(chunk for chunk in resp.iter_content(8192) if chunk)

    # -------------------------
    # powershell backend
    # -------------------------
    def _powershell_command(self, url: str, output_path: Path, timeout: int = 60, headers: dict = None) -> str:
        headers = {
            "Cookie": "cookie1=value1; cookie2=value2",
            "User-Agent": "MyCustomAgent/1.0",
            "Authorization": "Bearer mytoken",
        }
        headers_str = ""

        if headers:
            headers_str = (
                "    -Headers @{ " + " ; ".join([f"'{key}' = '{value}'" for key, value in headers.items()]) + " }"
            )

        return rf"""
    $ErrorActionPreference = 'Stop'
    try {{
        Invoke-WebRequest `
            -Uri "{url}" `
            -OutFile "{str(output_path)}" `
            -UseBasicParsing `
            -TimeoutSec {timeout} `
            -ErrorAction Stop `
            {headers_str}
    }}
    catch {{
        Write-Host "[PS] Download failed: $($_.Exception.Message)" -ForegroundColor Red
        exit 1
    }}
    """

    def _download_powershell_to_file(self, url: str, output_path: Path) -> None:
        """
        Downloads a file using PowerShell's Invoke-WebRequest.
        Shows a simple Python spinner and prints errors if download fails.
        """
        cmd = ["powershell", "-NoProfile", "-Command", self._powershell_command(url, output_path, timeout=360)]

        spinner_chars = "|/-\\"
        spinner_index = 0

        # Start PowerShell process
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        # Spinner loop
        if not is_dumb_terminal():
            while proc.poll() is None:
                print(
                    f"\r[PS] Downloading {output_path.name} {spinner_chars[spinner_index % 4]}",
                    end="",
                    flush=True,
                )
                spinner_index += 1
                time.sleep(0.1)
        else:
            proc.wait()

        # Clear spinner line
        print("\r" + " " * (len(f"[PS] Downloading {output_path.name} /") + 5) + "\r", end="", flush=True)

        # Capture output
        stdout, stderr = proc.communicate()

        if proc.returncode != 0:
            if stdout:
                print(stdout, end="")
            if stderr:
                print(stderr, end="", file=sys.stderr)

            _logger.error("PowerShell download failed")
            raise RuntimeError(f"PowerShell download failed (exit code {proc.returncode})")

        # Print final completion message once
        print(f"[PS] Download complete: {output_path.name}")
        _logger.debug("Downloaded %s -> %s (powershell)", url, output_path)
