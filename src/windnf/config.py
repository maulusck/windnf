# config.py
import configparser
import logging
from pathlib import Path

_logger = logging.getLogger(__name__)


class Config:
    def __init__(self) -> None:
        self.config_dir = Path.home() / ".config" / "windnf"
        self.config_dir.mkdir(parents=True, exist_ok=True)
        self.config_path = self.config_dir / "windnf.conf"

        # --------------------
        # Default values
        # --------------------
        self.log_level: str = "info"
        self.downloader: str = "powershell"
        self.skip_ssl_verify: bool = True
        self.db_path: Path = self.config_dir / "windnf.sqlite"
        self.download_path: Path = Path(".")

        self.load()

    def load(self) -> None:
        parser = configparser.ConfigParser()

        if not self.config_path.exists():
            _logger.warning("Config file %s not found. Creating default config.", self.config_path)
            self._write_default_config()

        parser.read(self.config_path)

        general = parser["general"]

        self.log_level = general.get("log_level", self.log_level)
        self.downloader = general.get("downloader", self.downloader)
        self.skip_ssl_verify = general.getboolean("skip_ssl_verify", fallback=self.skip_ssl_verify)
        self.db_path = Path(general.get("db_path", self.db_path))

        dp = general.get("download_path", self.download_path)
        self.download_path = Path(dp)
        self.download_path.mkdir(parents=True, exist_ok=True)

    def _write_default_config(self) -> None:
        parser = configparser.ConfigParser()
        parser["general"] = {
            "log_level": self.log_level,
            "downloader": self.downloader,
            "skip_ssl_verify": str(self.skip_ssl_verify).lower(),
            "db_path": str(self.db_path),
            "download_path": str(self.download_path),
        }

        with self.config_path.open("w") as f:
            parser.write(f)

        _logger.info("Default config file written to %s", self.config_path)

    def save(self) -> None:
        parser = configparser.ConfigParser()
        parser["general"] = {
            "log_level": self.log_level,
            "downloader": self.downloader,
            "skip_ssl_verify": str(self.skip_ssl_verify).lower(),
            "db_path": str(self.db_path),
            "download_path": str(self.download_path),
        }

        with self.config_path.open("w") as f:
            parser.write(f)

        _logger.info("Config saved to %s", self.config_path)
