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
from .logger import setup_logger

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
    def _powershell_command(self, url: str, output_path: Path) -> str:
        return rf"""
    $ErrorActionPreference = 'Stop'

    $Url = '{url}'
    $OutFile = '{str(output_path)}'
    $Label = '[PS] Downloading {output_path.name}'
    $spinner = @('|','/','-','\')
    $i = 0

    # Start the download as a child process
    $download = Start-Process powershell -ArgumentList "-Command Invoke-WebRequest -Uri `"$Url`" -OutFile `"$OutFile`"" -NoNewWindow -PassThru

    # Spinner loop
    while (-not $download.HasExited) {{
        if (-not [Console]::IsOutputRedirected) {{
            Write-Host -NoNewline "`r$Label $($spinner[$i % 4])"
            Start-Sleep -Milliseconds 100
            $i++
        }} else {{
            Start-Sleep -Milliseconds 100
        }}
    }}

    Write-Host "`r[PS] Download complete{' ' * 10}"
    """

    def _download_powershell_to_file(self, url: str, output_path: Path) -> None:
        cmd = [
            "powershell",
            "-NoProfile",
            "-Command",
            self._powershell_command(url, output_path),
        ]

        try:
            subprocess.run(cmd, check=True)
        except subprocess.CalledProcessError as e:
            _logger.error("PowerShell download failed")
            raise RuntimeError("PowerShell download failed") from e

        _logger.debug("Downloaded %s -> %s (powershell)", url, output_path)
