# config.py
import configparser
from pathlib import Path

from .logger import setup_logger

_logger = setup_logger()


class Config:
    def __init__(self) -> None:
        self.config_dir = Path.home() / ".windnf"
        self.config_dir.mkdir(exist_ok=True)
        self.config_path = self.config_dir / "windnf.conf"

        # Default values
        self.downloader: str = "powershell"
        self.skip_ssl_verify: bool = True
        self.db_path: Path = self.config_dir / "windnf.sqlite"
        self.download_path: Path = Path(".")

        self.load()

    def load(self) -> None:
        parser = configparser.ConfigParser()
        if not self.config_path.exists():
            _logger.warning(f"Config file {self.config_path} not found. Creating default config.")
            self._write_default_config()

        parser.read(self.config_path)

        self.downloader = parser.get("general", "downloader", fallback=self.downloader)
        self.skip_ssl_verify = parser.getboolean("general", "skip_ssl_verify", fallback=self.skip_ssl_verify)
        self.db_path = Path(parser.get("general", "db_path", fallback=str(self.db_path)))

        dp = parser.get("general", "download_path", fallback=str(self.download_path))
        self.download_path = Path(dp)
        self.download_path.mkdir(parents=True, exist_ok=True)

    def _write_default_config(self) -> None:
        parser = configparser.ConfigParser()
        parser["general"] = {
            "downloader": self.downloader,
            "skip_ssl_verify": str(self.skip_ssl_verify).lower(),
            "db_path": str(self.db_path),
            "download_path": str(self.download_path),
        }
        with self.config_path.open("w") as f:
            parser.write(f)
        _logger.info(f"Default config file written to {self.config_path}")

    def save(self) -> None:
        parser = configparser.ConfigParser()
        parser["general"] = {
            "downloader": self.downloader,
            "skip_ssl_verify": str(self.skip_ssl_verify).lower(),
            "db_path": str(self.db_path),
            "download_path": str(self.download_path),
        }
        with self.config_path.open("w") as f:
            parser.write(f)
        _logger.info(f"Config saved to {self.config_path}")
