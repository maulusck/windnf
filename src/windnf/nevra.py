import functools
import re
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

# Regex supports names with dots, dashes, underscores, plus, digits.
# Accepts optional epoch in the form -E:version-release.arch
NEVRA_RE = re.compile(
    r"""
    ^
    (?P<name>[A-Za-z0-9._+\-]+)
    (?:-(?P<epoch>[0-9]+):)?                 # optional epoch (digits)
    -
    (?P<version>[A-Za-z0-9._+\-]+)
    -
    (?P<release>[A-Za-z0-9._+\-]+)
    \.
    (?P<arch>[A-Za-z0-9_.+]+)
    $
    """,
    re.VERBOSE,
)


def rpmvercmp(a: str, b: str) -> int:
    """
    Lightweight rpm-style version comparison.
    Returns:
      - negative if a < b
      - zero if equal
      - positive if a > b
    This is not a perfect reimplementation of rpmvercmp but is
    sufficient for sorting versions in common metadata.
    """

    def split_parts(s: str):
        # split into numeric and non-numeric chunks
        return re.findall(r"[0-9]+|[^0-9]+", s or "")

    def cmp_item(x, y):
        # compare numeric if both digits, else lexicographic
        if isinstance(x, int) and isinstance(y, int):
            return (x > y) - (x < y)
        return (str(x) > str(y)) - (str(x) < str(y))

    pa = split_parts(a or "")
    pb = split_parts(b or "")

    for xa, xb in zip(pa, pb):
        if xa.isdigit() and xb.isdigit():
            na, nb = int(xa), int(xb)
            if na != nb:
                return (na > nb) - (na < nb)
        else:
            if xa != xb:
                return (xa > xb) - (xa < xb)

    # if all zipped parts equal, longer sequence wins
    return (len(pa) > len(pb)) - (len(pa) < len(pb))


@functools.total_ordering
@dataclass(frozen=True)
class NEVRA:
    """
    NEVRA dataclass — holds name/epoch/version/release/arch and DB-related optional metadata.

    Usage:
      NEVRA.parse("bash-0:5.1.12-3.fc39.x86_64")
      NEVRA.from_row(db_row)
    """

    name: str
    epoch: Optional[str]
    version: Optional[str]
    release: Optional[str]
    arch: Optional[str]

    # Optional metadata fields (not part of canonical NEVRA key)
    pkgId: Optional[str] = None
    repo_id: Optional[int] = None
    src: bool = False

    # -----
    # Parsing / construction
    # -----
    @staticmethod
    def parse(s: str) -> "NEVRA":
        """
        Parse canonical NEVRA string like:
            name-version-release.arch
            name-epoch:version-release.arch
        Raises ValueError if invalid.
        """
        if not isinstance(s, str):
            raise ValueError("NEVRA.parse expects a string")

        s = s.strip()
        m = NEVRA_RE.match(s)
        if not m:
            raise ValueError(f"Invalid NEVRA string: {s}")
        d = m.groupdict()
        arch = d.get("arch")
        src = arch in ("src", "nosrc")
        return NEVRA(
            name=d.get("name"),
            epoch=d.get("epoch"),
            version=d.get("version"),
            release=d.get("release"),
            arch=arch,
            src=src,
        )

    @staticmethod
    def from_row(row: Dict[str, Any]) -> "NEVRA":
        """
        Construct NEVRA from a DB row (mapping or sqlite3.Row) that has
        columns: name, epoch, version, release, arch, optional pkgId and repo_id.
        """
        if row is None:
            raise ValueError("row must not be None")
        return NEVRA(
            name=row["name"],
            epoch=row.get("epoch"),
            version=row.get("version"),
            release=row.get("release"),
            arch=row.get("arch"),
            pkgId=row.get("pkgId"),
            repo_id=row.get("repo_id"),
            src=(row.get("arch") in ("src", "nosrc")),
        )

    @staticmethod
    def from_rpm_filename(filename: str) -> "NEVRA":
        """
        Try to infer NEVRA from an RPM filename like:
            foo-1.2.3-4.x86_64.rpm or foo-1.2.3-4.src.rpm
        This is a best-effort helper.
        """
        if filename.endswith(".rpm"):
            filename = filename[:-4]
        # strip path
        fname = filename.split("/")[-1].split("\\")[-1]
        # try matching NEVRA pattern directly
        try:
            return NEVRA.parse(fname)
        except ValueError:
            # fallback: try to locate last ".arch" segment and parse
            if "." not in fname:
                raise ValueError(f"Cannot parse rpm filename into NEVRA: {filename}")
            arch = fname.split(".")[-1]
            base = ".".join(fname.split(".")[:-1])
            try:
                return NEVRA.parse(f"{base}.{arch}")
            except ValueError:
                raise ValueError(f"Cannot parse rpm filename into NEVRA: {filename}")

    # -----
    # String forms
    # -----
    def __str__(self) -> str:
        e = f"{self.epoch}:" if self.epoch else ""
        return f"{self.name}-{e}{self.version}-{self.release}.{self.arch}"

    def to_nvr(self) -> str:
        """Return name-version-release (no epoch/arch)."""
        return f"{self.name}-{self.version}-{self.release}"

    def to_nvra(self) -> str:
        """Return name-epoch:version-release.arch (canonical)."""
        e = f"{self.epoch}:" if self.epoch else ""
        return f"{self.name}-{e}{self.version}-{self.release}.{self.arch}"

    # -----
    # Ordering / comparison
    # -----
    def _cmp_tuple(self) -> Tuple:
        epoch_val = int(self.epoch) if (self.epoch and self.epoch.isdigit()) else 0
        return (self.name, epoch_val, self.version or "", self.release or "", self.arch or "")

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, NEVRA):
            return False
        return self._cmp_tuple() == other._cmp_tuple()

    def __lt__(self, other: "NEVRA") -> bool:
        if not isinstance(other, NEVRA):
            return NotImplemented

        # Name compare
        if self.name != other.name:
            return self.name < other.name

        # Epoch compare numeric
        e1 = int(self.epoch or 0)
        e2 = int(other.epoch or 0)
        if e1 != e2:
            return e1 < e2

        # Version compare with rpmvercmp
        c = rpmvercmp(self.version or "", other.version or "")
        if c != 0:
            return c < 0

        # Release compare with rpmvercmp
        c = rpmvercmp(self.release or "", other.release or "")
        if c != 0:
            return c < 0

        # Arch compare lexicographic
        return (self.arch or "") < (other.arch or "")

    # -----
    # DB helpers
    # -----
    def as_db_filters(self) -> Dict[str, Any]:
        """
        Convert to dict of non-None NEVRA fields, suitable to build SQL WHERE clauses.
        """
        out = {
            "name": self.name,
            "epoch": self.epoch,
            "version": self.version,
            "release": self.release,
            "arch": self.arch,
        }
        return {k: v for k, v in out.items() if v is not None}

    def matches_row(self, row: Dict[str, Any]) -> bool:
        """
        Check if row from packages table matches this NEVRA exactly (name, epoch, version, release, arch).
        """
        if row is None:
            return False
        return (
            row.get("name") == self.name
            and str(row.get("epoch") or "0") == str(self.epoch or "0")
            and row.get("version") == self.version
            and row.get("release") == self.release
            and row.get("arch") == self.arch
        )

    def is_source(self) -> bool:
        return self.src or (self.arch in ("src", "nosrc"))

    def is_binary(self) -> bool:
        return not self.is_source()
