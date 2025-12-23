import configparser
from pathlib import Path
from typing import Optional  # <--- FIXED: Added missing import

from .logger import setup_logger

_logger = setup_logger()


class Config:
    def __init__(self) -> None:
        self.config_dir = Path.home() / ".config" / "windnf"
        self.config_dir.mkdir(parents=True, exist_ok=True)
        self.config_path = self.config_dir / "windnf.conf"

        # Default values
        self.downloader: str = "python"
        self.db_path: Path = self.config_dir / "windnf.sqlite"
        self.download_path: Path = Path(".")

        # Network Defaults
        self.timeout_connect: int = 10
        self.timeout_read: int = 60
        self.retries: int = 3
        self.use_sspi: bool = True
        self.verify_ssl: bool = True
        self.proxy_url: Optional[str] = None
        self.ca_bundle: Optional[str] = None

        self.load()

    def load(self) -> None:
        parser = configparser.ConfigParser()
        if not self.config_path.exists():
            _logger.warning(f"Config file {self.config_path} not found. Creating default config.")
            self._write_default_config()

        parser.read(self.config_path)

        # [general]
        self.downloader = parser.get("general", "downloader", fallback=self.downloader)
        self.db_path = Path(parser.get("general", "db_path", fallback=str(self.db_path)))

        dp = parser.get("general", "download_path", fallback=str(self.download_path))
        self.download_path = Path(dp)
        self.download_path.mkdir(parents=True, exist_ok=True)

        # [network] - Robust parsing
        if parser.has_section("network"):
            self.timeout_connect = parser.getint("network", "timeout_connect", fallback=10)
            self.timeout_read = parser.getint("network", "timeout_read", fallback=60)
            self.retries = parser.getint("network", "retries", fallback=3)
            self.use_sspi = parser.getboolean("network", "use_sspi", fallback=True)
            self.verify_ssl = parser.getboolean("network", "verify_ssl", fallback=True)
            self.ca_bundle = parser.get("network", "ca_bundle", fallback=None)
            
            # Handle empty strings mapping to None
            p_url = parser.get("network", "proxy_url", fallback=None)
            self.proxy_url = p_url if p_url else None

        # Handle legacy 'skip_ssl_verify' if it exists in [general]
        if parser.has_option("general", "skip_ssl_verify"):
            self.verify_ssl = not parser.getboolean("general", "skip_ssl_verify")

    def _write_default_config(self) -> None:
        parser = configparser.ConfigParser()
        parser["general"] = {
            "downloader": self.downloader,
            "db_path": str(self.db_path),
            "download_path": str(self.download_path),
        }
        parser["network"] = {
            "timeout_connect": str(self.timeout_connect),
            "timeout_read": str(self.timeout_read),
            "retries": str(self.retries),
            "use_sspi": str(self.use_sspi).lower(),
            "verify_ssl": str(self.verify_ssl).lower(),
            "ca_bundle": self.ca_bundle or "",
            "proxy_url": self.proxy_url or "",
        }
        with self.config_path.open("w") as f:
            parser.write(f)
        _logger.info(f"Default config written to {self.config_path}")