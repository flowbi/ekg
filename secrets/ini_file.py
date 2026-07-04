"""
secrets/ini_file.py  –  INI file secrets provider.

Reads credentials from a plain-text INI file whose sections are named
after secret refs, matching the convention used by every other provider.

File format
───────────
    [prod/meta-db]
    username = meta_user
    password = s3cr3t
    host     = meta-db.internal
    port     = 5432
    dbname   = meta

    [prod/ekg-target]
    username  = neo4j
    password  = bolt_password
    endpoint  = bolt://neo4j.internal:7687

Section names correspond exactly to the secret_ref strings used in
DBConfig.secret_ref and GraphTargetConfig.options["secret_ref"].

File path resolution (first match wins)
────────────────────────────────────────
  1. Explicit path passed to IniSecretsProvider(path=…)
  2. Environment variable  EKG_CREDENTIALS_FILE
  3. ./credentials.ini         (current working directory)
  4. ~/.ekg/credentials.ini    (user home directory)

Security
────────
The file is plain text, so its filesystem permissions are the only
protection.  On POSIX systems this module checks the file mode at load
time and emits a WARNING if the file is readable by group or other
users, together with the exact chmod command to fix it.

Chain position
──────────────
Insert 'ini' between cloud providers and env vars in EKG_SECRET_PROVIDERS:
  EKG_SECRET_PROVIDERS=aws,vault,ini,env   (recommended)
  EKG_SECRET_PROVIDERS=ini,env             (local dev, no cloud)

Dependencies
────────────
stdlib only (configparser, os, pathlib, stat).
"""

from __future__ import annotations

import configparser
import logging
import os
import stat
from pathlib import Path
from typing import Dict, List, Optional

from secrets.base import SecretsProvider

log = logging.getLogger("ekg_etl.secrets.ini")

# Keys that are recognised and returned from an INI section.
# Any key present in the section but not in this list is still returned —
# this list is used only for documentation and to produce a helpful warning
# when mandatory keys are absent.
_EXPECTED_KEYS = {"username", "password", "host", "port", "dbname", "endpoint", "region"}

# Default search path (evaluated in order, first existing file wins)
_DEFAULT_SEARCH_PATH: List[Path] = [
    Path(os.environ.get("EKG_CREDENTIALS_FILE", "__sentinel__")),
    Path("credentials.ini"),
    Path.home() / ".ekg" / "credentials.ini",
]


def _resolve_path(explicit: Optional[Path]) -> Optional[Path]:
    """
    Return the first usable credentials file path, or None if none found.
    """
    candidates: List[Path] = (
        [explicit] if explicit is not None
        else _DEFAULT_SEARCH_PATH
    )
    for candidate in candidates:
        if candidate.name == "__sentinel__":
            # EKG_CREDENTIALS_FILE was not set; skip sentinel
            continue
        if candidate.exists() and candidate.is_file():
            return candidate
    return None


def _check_permissions(path: Path) -> None:
    """
    On POSIX systems, warn if the credentials file is group- or world-readable.
    Logs a WARNING with the exact chmod command to fix the permissions.
    On Windows, logs an INFO noting that permission checks are unavailable.
    """
    try:
        mode = path.stat().st_mode
    except OSError:
        return

    # os.name == 'nt' on Windows; stat module permission bits are POSIX concepts
    if os.name == "nt":
        log.info(
            "Credentials file '%s' loaded.  "
            "Windows file permission checks are not supported; "
            "ensure the file is protected by NTFS ACLs.",
            path,
        )
        return

    # Check group-read (S_IRGRP) and other-read (S_IROTH)
    group_readable = bool(mode & stat.S_IRGRP)
    other_readable = bool(mode & stat.S_IROTH)

    if other_readable:
        log.warning(
            "SECURITY: credentials file '%s' is world-readable (mode %04o).  "
            "Any user on this system can read your passwords.  "
            "Fix immediately:  chmod 600 '%s'",
            path, stat.S_IMODE(mode), path,
        )
    elif group_readable:
        log.warning(
            "SECURITY: credentials file '%s' is group-readable (mode %04o).  "
            "Members of the file's group can read your passwords.  "
            "Consider restricting:  chmod 600 '%s'",
            path, stat.S_IMODE(mode), path,
        )
    else:
        log.debug(
            "Credentials file '%s' permissions OK (mode %04o).",
            path, stat.S_IMODE(mode),
        )


class IniSecretsProvider(SecretsProvider):
    """
    Reads credentials from a plain-text INI file.

    Parameters
    ----------
    path : Path | None
        Explicit path to the credentials file.  When None, the default
        search path is used (see module docstring).

    Behaviour
    ---------
    • The file is parsed once at first use and cached for the provider lifetime.
    • Section names are matched case-insensitively against the ref string.
    • All key=value pairs in the matching section are returned as strings.
    • Returns None (not raises) when the section is not found, allowing the
      chain to fall through to the next provider (env vars).
    • Raises RuntimeError if the file was found but cannot be parsed.
    """

    def __init__(self, path: Optional[Path] = None) -> None:
        self._explicit_path = Path(path) if path is not None else None
        self._parser:   Optional[configparser.ConfigParser] = None
        self._filepath: Optional[Path] = None

    def _load(self) -> None:
        """Lazy-load the INI file on first resolve() call."""
        if self._parser is not None:
            return   # already loaded

        resolved = _resolve_path(self._explicit_path)
        if resolved is None:
            paths_tried = (
                [str(self._explicit_path)] if self._explicit_path
                else [str(p) for p in _DEFAULT_SEARCH_PATH if p.name != "__sentinel__"]
            )
            log.info(
                "IniSecretsProvider: no credentials file found.  "
                "Searched: %s.  "
                "This provider will return None for all refs.",
                ", ".join(f"'{p}'" for p in paths_tried),
            )
            # Install an empty parser so subsequent calls skip the search
            self._parser = configparser.ConfigParser()
            return

        _check_permissions(resolved)

        parser = configparser.ConfigParser(
            # Preserve key casing (username, not Username)
            # configparser lowercases keys by default; override with str identity
            optionxform=str,
            # Allow values that contain = (e.g. base64 tokens)
            strict=True,
            interpolation=None,
        )
        try:
            read_ok = parser.read(str(resolved), encoding="utf-8")
            if not read_ok:
                raise RuntimeError(f"configparser could not read '{resolved}'")
        except configparser.Error as exc:
            raise RuntimeError(
                f"IniSecretsProvider: failed to parse '{resolved}': {exc}"
            ) from exc

        self._parser   = parser
        self._filepath = resolved
        log.info(
            "IniSecretsProvider: loaded %d section(s) from '%s'.",
            len(parser.sections()), resolved,
        )

    def resolve(self, ref: str) -> Optional[Dict[str, str]]:
        self._load()

        # configparser stores section names as-is (case-sensitive by default
        # for sections).  We do a case-insensitive match so that
        # "prod/meta-db" and "PROD/META-DB" both work.
        matched_section: Optional[str] = None
        for section in self._parser.sections():
            if section.lower() == ref.lower():
                matched_section = section
                break

        if matched_section is None:
            log.debug("IniSecretsProvider: section '%s' not found in '%s'.",
                      ref, self._filepath)
            return None

        result = dict(self._parser[matched_section])

        # Warn if mandatory keys are absent (helpful during initial setup)
        missing = {"username", "password"} - result.keys()
        if missing:
            log.warning(
                "IniSecretsProvider: section '[%s]' in '%s' is missing "
                "required key(s): %s.  Credentials may be incomplete.",
                matched_section, self._filepath,
                ", ".join(sorted(missing)),
            )

        log.debug("IniSecretsProvider: resolved section '[%s]' from '%s'.",
                  matched_section, self._filepath)
        return result
