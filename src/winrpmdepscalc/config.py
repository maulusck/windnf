import tempfile
from pathlib import Path

import yaml


class Config:
    def __init__(self) -> None:
        # Base directories
        self.BASE_DIR: Path = Path.home() / ".winrpmdepscalc"
        self.DOWNLOAD_DIR: Path = self.BASE_DIR / "rpms"

        # Decide which config file to use (cwd config if exists, otherwise BASE_DIR/config.yaml)
        cwd_config = Path.cwd() / "config.yaml"
        self.CONFIG_FILE: Path = (
            cwd_config if cwd_config.exists() else self.BASE_DIR / "config.yaml"
        )

        # Repo info
        self.REPO_BASE_URL: str = (
            "https://dl.fedoraproject.org/pub/epel/9/Everything/x86_64/"
        )
        self.REPOMD_XML: str = "repodata/repomd.xml"

        # Temp download files
        self.TEMP_DOWNLOAD_DIR: Path = Path(tempfile.gettempdir())
        self.LOCAL_REPOMD_FILE: Path = self.TEMP_DOWNLOAD_DIR / "repomd.xml"
        self.LOCAL_XZ_FILE: Path = self.TEMP_DOWNLOAD_DIR / "primary.xml.xz"
        self.LOCAL_XML_FILE: Path = self.TEMP_DOWNLOAD_DIR / "primary.xml"

        # Display settings
        self.PACKAGE_COLUMNS: int = 4
        self.PACKAGE_COLUMN_WIDTH: int = 30

        # Behavior flags
        self.SKIP_SSL_VERIFY: bool = True
        self.SUPPORT_WEAK_DEPS: bool = False
        self.ONLY_LATEST_VERSION: bool = True
        self.DOWNLOADER: str = "powershell"

        # Ensure directories exist
        self.BASE_DIR.mkdir(parents=True, exist_ok=True)
        self.DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

    def update_from_dict(self, data: dict) -> None:
        for key, value in data.items():
            key_upper = key.upper()
            if hasattr(self, key_upper):
                current_value = getattr(self, key_upper)
                if key_upper == "TEMP_DOWNLOAD_DIR":
                    temp_dir = Path(value)
                    self.TEMP_DOWNLOAD_DIR = temp_dir
                    self.LOCAL_REPOMD_FILE = temp_dir / "repomd.xml"
                    self.LOCAL_XZ_FILE = temp_dir / "primary.xml.xz"
                    self.LOCAL_XML_FILE = temp_dir / "primary.xml"
                else:
                    setattr(
                        self,
                        key_upper,
                        Path(value) if isinstance(current_value, Path) else value,
                    )

    def to_dict(self) -> dict:
        return {
            k: (
                str(getattr(self, k))
                if isinstance(getattr(self, k), Path)
                else getattr(self, k)
            )
            for k in dir(self)
            if k.isupper()
        }

    def save_to_file(self, path: Path = None):
        path = path or self.CONFIG_FILE
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            yaml.safe_dump(self.to_dict(), f)

    def load_from_file(self, path: Path = None):
        path = path or self.CONFIG_FILE
        if path.exists():
            with open(path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f)
                if data:
                    self.update_from_dict(data)
