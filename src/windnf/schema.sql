----------------------------------------------------------------------
-- REPOSITORIES (Custom Extension)
----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS repositories (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL,
    base_url TEXT NOT NULL,
    repomd_url TEXT NOT NULL,
    type TEXT NOT NULL CHECK(type IN ('binary', 'source')),
    source_repo_id INTEGER REFERENCES repositories(id) ON DELETE SET NULL,
    last_updated TEXT
);

----------------------------------------------------------------------
-- PRIMARY METADATA (Enhanced primary.sqlite structure)
----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS packages (
    pkgKey INTEGER PRIMARY KEY,
    repo_id INTEGER NOT NULL REFERENCES repositories(id) ON DELETE CASCADE,

    pkgId TEXT,
    name TEXT,
    arch TEXT,
    version TEXT,
    epoch TEXT,
    release TEXT,
    summary TEXT,
    description TEXT,
    url TEXT,
    time_file INTEGER,
    time_build INTEGER,
    rpm_license TEXT,
    rpm_vendor TEXT,
    rpm_group TEXT,
    rpm_buildhost TEXT,
    rpm_sourcerpm TEXT,
    rpm_header_start INTEGER,
    rpm_header_end INTEGER,
    rpm_packager TEXT,
    size_package INTEGER,
    size_installed INTEGER,
    size_archive INTEGER,
    location_href TEXT,
    location_base TEXT,
    checksum_type TEXT
);

----------------------------------------------------------------------
-- PRIMARY: FILES TABLE
----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    type TEXT,
    pkgKey INTEGER REFERENCES packages(pkgKey) ON DELETE CASCADE
);

----------------------------------------------------------------------
-- PRIMARY: RELATION TABLES
----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS requires (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    flags TEXT,
    epoch TEXT,
    version TEXT,
    release TEXT,
    pkgKey INTEGER REFERENCES packages(pkgKey) ON DELETE CASCADE,
    pre INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS provides (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    flags TEXT,
    epoch TEXT,
    version TEXT,
    release TEXT,
    pkgKey INTEGER REFERENCES packages(pkgKey) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS conflicts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    flags TEXT,
    epoch TEXT,
    version TEXT,
    release TEXT,
    pkgKey INTEGER REFERENCES packages(pkgKey) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS obsoletes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    flags TEXT,
    epoch TEXT,
    version TEXT,
    release TEXT,
    pkgKey INTEGER REFERENCES packages(pkgKey) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS suggests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    flags TEXT,
    epoch TEXT,
    version TEXT,
    release TEXT,
    pkgKey INTEGER REFERENCES packages(pkgKey) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS enhances (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    flags TEXT,
    epoch TEXT,
    version TEXT,
    release TEXT,
    pkgKey INTEGER REFERENCES packages(pkgKey) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS recommends (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    flags TEXT,
    epoch TEXT,
    version TEXT,
    release TEXT,
    pkgKey INTEGER REFERENCES packages(pkgKey) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS supplements (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    flags TEXT,
    epoch TEXT,
    version TEXT,
    release TEXT,
    pkgKey INTEGER REFERENCES packages(pkgKey) ON DELETE CASCADE
);

----------------------------------------------------------------------
-- FILELISTS METADATA (filelists.sqlite)
----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS filelist (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pkgKey INTEGER REFERENCES packages(pkgKey) ON DELETE CASCADE,
    dirname TEXT,
    filenames TEXT,
    filetypes TEXT
);

----------------------------------------------------------------------
-- OTHER METADATA (changelog, from other.sqlite)
----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS changelog (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pkgKey INTEGER REFERENCES packages(pkgKey) ON DELETE CASCADE,
    author TEXT,
    date INTEGER,
    changelog TEXT
);

----------------------------------------------------------------------
-- UNIFIED CLEANUP TRIGGER (primary + filelists + other)
----------------------------------------------------------------------
CREATE TRIGGER IF NOT EXISTS removals
BEFORE DELETE ON packages
BEGIN
    DELETE FROM files        WHERE pkgKey = OLD.pkgKey;
    DELETE FROM requires     WHERE pkgKey = OLD.pkgKey;
    DELETE FROM provides     WHERE pkgKey = OLD.pkgKey;
    DELETE FROM conflicts    WHERE pkgKey = OLD.pkgKey;
    DELETE FROM obsoletes    WHERE pkgKey = OLD.pkgKey;
    DELETE FROM suggests     WHERE pkgKey = OLD.pkgKey;
    DELETE FROM enhances     WHERE pkgKey = OLD.pkgKey;
    DELETE FROM recommends   WHERE pkgKey = OLD.pkgKey;
    DELETE FROM supplements  WHERE pkgKey = OLD.pkgKey;

    DELETE FROM filelist     WHERE pkgKey = OLD.pkgKey;
    DELETE FROM changelog    WHERE pkgKey = OLD.pkgKey;
END;

----------------------------------------------------------------------
-- INDEXES
----------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_repo_name ON repositories (name);

-- packages
CREATE INDEX IF NOT EXISTS packagename ON packages (name);
CREATE INDEX IF NOT EXISTS packageId ON packages (pkgId);
CREATE INDEX IF NOT EXISTS pkg_repo ON packages (repo_id);

-- primary: files + relations
CREATE INDEX IF NOT EXISTS filenames ON files (name);
CREATE INDEX IF NOT EXISTS pkgfiles ON files (pkgKey);

CREATE INDEX IF NOT EXISTS pkgrequires ON requires (pkgKey);
CREATE INDEX IF NOT EXISTS requiresname ON requires (name);

CREATE INDEX IF NOT EXISTS pkgprovides ON provides (pkgKey);
CREATE INDEX IF NOT EXISTS providesname ON provides (name);

CREATE INDEX IF NOT EXISTS pkgconflicts ON conflicts (pkgKey);
CREATE INDEX IF NOT EXISTS pkgobsoletes ON obsoletes (pkgKey);
CREATE INDEX IF NOT EXISTS pkgsuggests ON suggests (pkgKey);
CREATE INDEX IF NOT EXISTS pkgenhances ON enhances (pkgKey);
CREATE INDEX IF NOT EXISTS pkgrecommends ON recommends (pkgKey);
CREATE INDEX IF NOT EXISTS pkgsupplements ON supplements (pkgKey);

-- filelists
CREATE INDEX IF NOT EXISTS keyfile ON filelist (pkgKey);
CREATE INDEX IF NOT EXISTS dirnames ON filelist (dirname);

-- other (changelog)
CREATE INDEX IF NOT EXISTS keychange ON changelog (pkgKey);
