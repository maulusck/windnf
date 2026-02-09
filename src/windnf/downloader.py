# downloader.py
from __future__ import annotations

import logging
import subprocess
import sys
import tempfile
import time
from enum import Enum
from pathlib import Path
from typing import Union

import requests
import urllib3
from requests.adapters import HTTPAdapter, Retry
from tqdm import tqdm

from .config import Config
from .logger import is_dumb_terminal

log = logging.getLogger(__name__)


# -------------------------
# Exceptions
# -------------------------
class DownloadError(RuntimeError):
    """Raised when a download fails for any reason."""


# -------------------------
# Downloader backend type
# -------------------------
class DownloaderType(Enum):
    POWERSHELL = "powershell"
    PYTHON = "python"

    @classmethod
    def has_value(cls, value: str) -> bool:
        return value and value.lower() in (item.value for item in cls)


# -------------------------
# Downloader
# -------------------------
class Downloader:
    def __init__(self, config: Config) -> None:
        self.config = config

        backend = getattr(config, "downloader", "python").lower()
        if not DownloaderType.has_value(backend):
            raise ValueError(f"Invalid downloader backend: {backend}")

        self.backend = DownloaderType(backend)

        proxy_url = getattr(config, "proxy_url", None)
        skip_ssl_verify = getattr(config, "skip_ssl_verify", True)

        if skip_ssl_verify:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            log.warning("SSL verification is disabled. This may expose you to security risks.")

        self.session: requests.Session | None = None

        if self.backend is DownloaderType.PYTHON:
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

    # -------------------------
    # Public API
    # -------------------------
    def download_to_file(self, url: str, output_path: Union[str, Path]) -> None:
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        if output_path.exists():
            log.info("File already exists: %s", output_path)
            return

        if self.backend is DownloaderType.PYTHON:
            self._download_python_to_file(url, output_path)
        else:
            self._download_powershell_to_file(url, output_path)

    def download_to_memory(self, url: str) -> bytes:
        if self.backend is DownloaderType.PYTHON:
            return self._download_python_to_memory(url)

        tmp = tempfile.NamedTemporaryFile(delete=False)
        tmp.close()

        try:
            self._download_powershell_to_file(url, Path(tmp.name))
            return Path(tmp.name).read_bytes()
        finally:
            Path(tmp.name).unlink(missing_ok=True)

    # -------------------------
    # Python backend
    # -------------------------
    def _download_python_to_file(self, url: str, output_path: Path) -> None:
        assert self.session is not None

        try:
            with self.session.get(url, stream=True, timeout=60) as resp:
                resp.raise_for_status()
                total = int(resp.headers.get("content-length") or 0)

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

        except KeyboardInterrupt:
            output_path.unlink(missing_ok=True)
            raise

        except Exception as exc:
            output_path.unlink(missing_ok=True)
            raise DownloadError(f"Download failed: {url}") from exc

        log.debug("Downloaded %s -> %s (python)", url, output_path)

    def _download_python_to_memory(self, url: str) -> bytes:
        assert self.session is not None

        try:
            with self.session.get(url, stream=True, timeout=60) as resp:
                resp.raise_for_status()
                return b"".join(chunk for chunk in resp.iter_content(8192) if chunk)

        except KeyboardInterrupt:
            raise

        except Exception as exc:
            raise DownloadError(f"Download failed: {url}") from exc

    # -------------------------
    # PowerShell backend
    # -------------------------
    def _powershell_script(self, url: str, output_path: Path, timeout: int) -> str:
        return rf"""
$ErrorActionPreference = 'Stop'
try {{
    Invoke-WebRequest `
        -Uri "{url}" `
        -OutFile "{str(output_path)}" `
        -UseBasicParsing `
        -TimeoutSec {timeout}
}}
catch {{
    Write-Error $_.Exception.Message
    exit 1
}}
"""

    def _download_powershell_to_file(self, url: str, output_path: Path) -> None:
        cmd = [
            "powershell",
            "-NoProfile",
            "-Command",
            self._powershell_script(url, output_path, timeout=360),
        ]

        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        spinner = "|/-\\"
        i = 0

        try:
            if not is_dumb_terminal():
                while proc.poll() is None:
                    print(
                        f"\r[PS] Downloading {output_path.name} {spinner[i % 4]}",
                        end="",
                        flush=True,
                    )
                    i += 1
                    time.sleep(0.1)
            else:
                proc.wait()

        except KeyboardInterrupt:
            proc.kill()
            proc.wait()
            output_path.unlink(missing_ok=True)
            raise

        finally:
            print("\r" + " " * 80 + "\r", end="", flush=True)

        stdout, stderr = proc.communicate()

        if proc.returncode != 0:
            output_path.unlink(missing_ok=True)

            if stdout:
                print(stdout, end="")
            if stderr:
                print(stderr, end="", file=sys.stderr)

            raise DownloadError(f"PowerShell download failed (exit code {proc.returncode})")

        print(f"[PS] Download complete: {output_path.name}")
        log.debug("Downloaded %s -> %s (powershell)", url, output_path)
