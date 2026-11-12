"""Zero-friction on-demand GGUF provisioning for med-agent-hub profiles.

When ``LLAMA_MODEL_DIR`` and ``LLAMA_MODELS_MANIFEST`` are configured (for
example via Docker Compose mounting ``llama-router.models.tsv`` read-only and
``~/.cache/llama-router-models`` at ``/app/models``), ``med-agent-hub``
verifies whether a profile's required local GGUF file is already present on
disk before dispatching to ``llama-router``. If a pinned model is named in a
profile (or requested via a dynamic ``wahs-kb:<model>`` / ``wahs-raw:<model>``
leg) and its ``<alias>.gguf`` file is not yet downloaded, ``med-agent-hub``
automatically streams it from the pinned Hugging Face revision URL, verifies
its SHA-256 digest, and atomically installs ``<alias>.gguf`` into
``LLAMA_MODEL_DIR`` so ``llama-router`` can load it on demand without failing.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional

import httpx

logger = logging.getLogger(__name__)

_PROVISION_LOCKS: Dict[str, asyncio.Lock] = {}
_MANIFEST_CACHE: Dict[ tuple[str, int, int], Dict[str, "PinnedModelRecord"] ] = {}


@dataclass(frozen=True)
class PinnedModelRecord:
    alias: str
    filename: str
    sha256: str
    source_url: str


def _model_dir() -> Optional[Path]:
    raw = (os.environ.get("LLAMA_MODEL_DIR") or "").strip()
    if not raw:
        return None
    path = Path(raw).expanduser()
    return path if path.is_dir() else None


def _manifest_path() -> Optional[Path]:
    raw = (os.environ.get("LLAMA_MODELS_MANIFEST") or "").strip()
    if raw:
        path = Path(raw).expanduser()
        return path if path.is_file() else None
    if _model_dir() is None:
        return None
    bundled = Path(__file__).resolve().with_name("llama-router.models.tsv")
    return bundled if bundled.is_file() else None


def parse_preload_models(raw: str) -> list[str]:
    """Parse a comma/whitespace-separated list of model aliases for warmup."""
    seen: list[str] = []
    for part in str(raw or "").replace("\n", ",").split(","):
        alias = part.strip()
        if alias and alias not in seen:
            seen.append(alias)
    return seen


def load_pinned_manifest() -> Dict[str, PinnedModelRecord]:
    manifest = _manifest_path()
    if manifest is None:
        return {}
    try:
        st = manifest.stat()
        cache_key = (str(manifest), st.st_mtime_ns, st.st_size)
        cached = _MANIFEST_CACHE.get(cache_key)
        if cached is not None:
            return cached
    except OSError:
        cache_key = None
    records: Dict[str, PinnedModelRecord] = {}
    try:
        for line in manifest.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            parts = stripped.split("\t")
            if len(parts) < 4:
                continue
            alias, filename, sha256, source_url = (
                parts[0].strip(),
                parts[1].strip(),
                parts[2].strip(),
                parts[3].strip(),
            )
            if alias and filename and len(sha256) == 64 and source_url:
                records[alias] = PinnedModelRecord(
                    alias=alias,
                    filename=filename,
                    sha256=sha256.lower(),
                    source_url=source_url,
                )
        if cache_key is not None:
            _MANIFEST_CACHE.clear()
            _MANIFEST_CACHE[cache_key] = records
    except OSError as exc:
        logger.warning("Failed to read LLAMA_MODELS_MANIFEST %s: %s", manifest, exc)
    return records


def is_model_file_present(alias: str) -> Optional[bool]:
    """Return True/False if LLAMA_MODEL_DIR is configured, else None."""
    model_dir = _model_dir()
    if model_dir is None:
        return None
    direct = model_dir / f"{alias}.gguf"
    if direct.is_file():
        return True
    records = load_pinned_manifest()
    rec = records.get(alias)
    if rec and (model_dir / rec.filename).is_file():
        return True
    return False


def is_model_pinned(alias: str) -> bool:
    return alias in load_pinned_manifest()


async def ensure_local_model_provisioned(alias: str) -> bool:
    """Ensure ``<alias>.gguf`` exists in ``LLAMA_MODEL_DIR``, downloading if pinned.

    Returns True if the model file is confirmed present (or downloaded), False
    if ``LLAMA_MODEL_DIR`` / ``LLAMA_MODELS_MANIFEST`` is unconfigured or the
    alias is not pinned in the manifest.
    """
    model_dir = _model_dir()
    if model_dir is None:
        return False
    target = model_dir / f"{alias}.gguf"
    if target.is_file():
        return True

    records = load_pinned_manifest()
    rec = records.get(alias)
    if rec is None:
        return False

    alt = model_dir / rec.filename
    if alt.is_file() and alt != target:
        try:
            target.symlink_to(rec.filename)
            return True
        except OSError:
            return True

    lock = _PROVISION_LOCKS.setdefault(alias, asyncio.Lock())
    async with lock:
        if target.is_file():
            return True
        partial = model_dir / f"{alias}.gguf.partial"
        logger.info(
            "Auto-provisioning missing GGUF for profile model %s (%s) from %s",
            alias,
            rec.filename,
            rec.source_url,
        )
        hasher = hashlib.sha256()
        try:
            async with httpx.AsyncClient(
                follow_redirects=True, timeout=httpx.Timeout(1800.0, connect=30.0)
            ) as client:
                async with client.stream("GET", rec.source_url) as resp:
                    resp.raise_for_status()
                    with open(partial, "wb") as fh:
                        async for chunk in resp.aiter_bytes(chunk_size=1024 * 1024):
                            if chunk:
                                fh.write(chunk)
                                hasher.update(chunk)
        except Exception as exc:
            partial.unlink(missing_ok=True)
            logger.error("Failed to auto-download GGUF for %s: %s", alias, exc)
            raise RuntimeError(
                f"Failed to auto-download model {alias!r} from {rec.source_url}: {exc}"
            ) from exc

        actual_sha = hasher.hexdigest().lower()
        if actual_sha != rec.sha256:
            partial.unlink(missing_ok=True)
            raise RuntimeError(
                f"Checksum mismatch for auto-downloaded model {alias!r}: "
                f"expected {rec.sha256}, got {actual_sha}"
            )
        partial.replace(target)
        logger.info("Installed verified GGUF %s -> %s (%s)", alias, target, actual_sha)
        return True
