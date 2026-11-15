"""Dynamic byte-budget LRU GPU memory management for llama.cpp Router Mode."""

from __future__ import annotations

import asyncio
import logging
import os
import re
import subprocess
import sys
import time
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx
import psutil

from . import model_provisioner

logger = logging.getLogger(__name__)

# Measured GGUF weights (MiB) for SHA-256-pinned models in server/llama-router.models.tsv.
PINNED_GGUF_WEIGHT_MIB: Dict[str, int] = {
    "gemma-e2b": 2963,
    "gemma-e4b": 4747,
    "gemma-e4b-q8": 7813,
    "gemma-4-12b-q4": 6792,
    "gemma-4-12b": 12083,
    "gemma-26b": 16162,
    "gemma-26b-a4b": 16162,
    "gemma-31b": 17475,
    "medgemma-1.5-4b": 3939,
    "medgemma-1.5-4b-q8": 3939,
    "medgemma-27b": 15780,
    "qwen3-4b": 2382,
    "qwen3.5-9b": 9086,
    "qwen2.5-14b": 8572,
    "qwen3-14b": 8585,
    "qwen3.6-35b": 21109,
    "qwen3.8-27b": 16746,
    "qwen2.5-coder-1.5b-instruct-q4_k_m": 1050,
    "llama-3.1-8b": 4693,
    "mistral-nemo-12b": 7131,
    "granite-3.3-8b": 4714,
    "lfm2-8b-a1b": 4811,
}

_PARAM_BILLIONS_RE = re.compile(r"(\d+(?:\.\d+)?)b", re.IGNORECASE)
_COMPUTE_OVERHEAD_MIB = 1024
_DEFAULT_CTX_TOKENS = 24576
_DEFAULT_UNKNOWN_WEIGHT_MIB = 12288
_DISABLED_BUDGET_TOKENS = frozenset({"0", "off", "false", "disabled", "none"})

_LAST_USED_BY_BASE: Dict[str, Dict[str, float]] = {}


def normalize_router_base_url(base_url: str) -> str:
    """Return the router root URL (without trailing slash or ``/v1``)."""
    clean = str(base_url or "").strip().rstrip("/")
    if clean.endswith("/v1"):
        clean = clean[:-3].rstrip("/")
    return clean


def reset_state() -> None:
    """Clear tracked per-model LRU timestamps (intended for isolated tests)."""
    _LAST_USED_BY_BASE.clear()


def touch_model(
    base_url: str, alias: str, *, timestamp: Optional[float] = None
) -> None:
    """Record a monotonic last-used timestamp for ``alias`` on ``base_url``."""
    root = normalize_router_base_url(base_url)
    clean_alias = str(alias or "").strip()
    if not root or not clean_alias:
        return
    now = time.monotonic() if timestamp is None else float(timestamp)
    bucket = _LAST_USED_BY_BASE.setdefault(root, {})
    prev = bucket.get(clean_alias, 0.0)
    bucket[clean_alias] = max(prev, now)


def _local_gguf_size_mib(alias: str) -> Optional[int]:
    model_dir = model_provisioner._model_dir()
    if model_dir is None:
        return None
    candidates: List[Path] = [model_dir / f"{alias}.gguf"]
    records = model_provisioner.load_pinned_manifest()
    rec = records.get(alias)
    if rec is not None:
        candidates.append(model_dir / rec.filename)
    for path in candidates:
        try:
            if path.is_file():
                return max(1, int(path.stat().st_size // (1024 * 1024)))
        except OSError:
            continue
    return None


def estimate_weights_mib(alias: str) -> int:
    """Estimate a model alias's GGUF weight footprint in MiB."""
    clean = str(alias or "").strip()
    on_disk = _local_gguf_size_mib(clean)
    if on_disk is not None:
        return on_disk
    pinned = PINNED_GGUF_WEIGHT_MIB.get(clean)
    if pinned is not None:
        return pinned
    match = _PARAM_BILLIONS_RE.search(clean)
    if match:
        params_b = float(match.group(1))
        multiplier = 1.05 if "q8" in clean.lower() else 0.62
        return max(512, int(params_b * 1024 * multiplier))
    return _DEFAULT_UNKNOWN_WEIGHT_MIB


def _extract_router_runtime_args(
    model_entry: Optional[Mapping[str, Any]],
) -> tuple[int, bool]:
    ctx_tokens = _DEFAULT_CTX_TOKENS
    swa_full = False
    if not isinstance(model_entry, Mapping):
        return ctx_tokens, swa_full
    status = model_entry.get("status")
    if not isinstance(status, Mapping):
        return ctx_tokens, swa_full
    raw_args = status.get("args")
    if not isinstance(raw_args, (list, tuple)):
        return ctx_tokens, swa_full
    args = [str(item) for item in raw_args]
    for idx, item in enumerate(args):
        if item == "--swa-full":
            swa_full = True
        elif item in ("--ctx-size", "-c") and idx + 1 < len(args):
            try:
                parsed = int(args[idx + 1])
                if parsed > 0:
                    ctx_tokens = parsed
            except (TypeError, ValueError):
                pass
    return ctx_tokens, swa_full


def estimate_model_footprint_mib(
    alias: str, model_entry: Optional[Mapping[str, Any]] = None
) -> int:
    """Estimate total GPU memory footprint (weights + compute buffer + KV cache)."""
    weights_mib = estimate_weights_mib(alias)
    ctx_tokens, swa_full = _extract_router_runtime_args(model_entry)
    if weights_mib < 6144:
        kv_mib_per_1k = 24
    elif swa_full:
        kv_mib_per_1k = 64
    else:
        kv_mib_per_1k = 40
    kv_mib = int((ctx_tokens / 1024.0) * kv_mib_per_1k)
    return weights_mib + _COMPUTE_OVERHEAD_MIB + kv_mib


def _nvidia_gpu_memory_mib() -> Optional[int]:
    try:
        output = subprocess.check_output(
            [
                "nvidia-smi",
                "--query-gpu=memory.total",
                "--format=csv,noheader,nounits",
            ],
            text=True,
            timeout=2.0,
            stderr=subprocess.DEVNULL,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    for line in output.splitlines():
        clean = line.strip()
        if not clean:
            continue
        try:
            mib = int(float(clean))
            if mib > 0:
                return mib
        except ValueError:
            continue
    return None


def auto_detect_vram_budget_mib(base_url: str = "") -> int:
    """Heuristically compute a safe resident GPU memory budget in MiB."""
    nvidia_mib = _nvidia_gpu_memory_mib()
    if nvidia_mib is not None:
        return int(nvidia_mib * 0.88)
    total_ram_mib = int(psutil.virtual_memory().total // (1024 * 1024))
    if sys.platform == "darwin":
        return int(total_ram_mib * 0.65)
    if "host.docker.internal" in str(base_url or "").lower():
        return int(total_ram_mib * 0.85)
    return int(total_ram_mib * 0.75)


def resolve_vram_budget_mib(base_url: str = "") -> int:
    """Resolve the active GPU byte budget in MiB (0 disables eviction)."""
    raw = (os.getenv("MED_AGENT_ROUTER_VRAM_BUDGET_MIB") or "").strip()
    if raw:
        if raw.lower() in _DISABLED_BUDGET_TOKENS:
            return 0
        try:
            parsed = int(raw)
        except ValueError:
            logger.warning(
                "Invalid MED_AGENT_ROUTER_VRAM_BUDGET_MIB=%r; falling back to auto-detect",
                raw,
            )
        else:
            return max(0, parsed)
    if os.getenv("PYTEST_CURRENT_TEST"):
        return 0
    return max(0, auto_detect_vram_budget_mib(base_url))


def _auth_headers(api_key: str) -> Dict[str, str]:
    headers: Dict[str, str] = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    return headers


def _is_llama_router_catalog(data: Any) -> bool:
    if not isinstance(data, list) or not data:
        return False
    return any(
        isinstance(item, Mapping)
        and isinstance(item.get("status"), Mapping)
        and isinstance(item["status"].get("value"), str)
        for item in data
    )


def _resident_status_value(item: Mapping[str, Any]) -> str:
    status = item.get("status")
    if not isinstance(status, Mapping):
        return ""
    return str(status.get("value") or "").strip().lower()


async def _wait_for_victims_unloaded(
    client: httpx.AsyncClient,
    models_url: str,
    headers: Dict[str, str],
    victims: List[str],
    *,
    poll_interval: float = 0.15,
    timeout_seconds: float = 10.0,
) -> None:
    if not victims:
        return
    victim_set = set(victims)
    deadline = time.monotonic() + timeout_seconds
    while True:
        try:
            resp = await client.get(models_url, headers=headers, timeout=3.0)
            resp.raise_for_status()
            payload = resp.json()
            data = payload.get("data") if isinstance(payload, Mapping) else None
            if not isinstance(data, list):
                return
            still_resident = {
                str(item.get("id"))
                for item in data
                if isinstance(item, Mapping)
                and str(item.get("id")) in victim_set
                and _resident_status_value(item) in {"loaded", "loading"}
            }
            if not still_resident:
                return
        except Exception as exc:
            logger.debug("router_memory unload confirmation probe failed: %s", exc)
            return
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            logger.warning(
                "router_memory: timed out waiting for unloaded models=%s to exit",
                sorted(still_resident),
            )
            return
        await asyncio.sleep(min(poll_interval, remaining))


async def ensure_router_capacity(
    target_model: str,
    *,
    base_url: str,
    api_key: str = "",
    is_remote: bool = False,
) -> List[str]:
    """Proactively unload LRU resident models when ``target_model`` exceeds budget.

    Caller must hold ``team._ROUTER_LOCK`` so in-flight requests are never
    evicted concurrently.
    """
    clean_target = str(target_model or "").strip()
    if is_remote or not clean_target:
        return []
    budget_mib = resolve_vram_budget_mib(base_url)
    if budget_mib <= 0:
        return []
    root_base = normalize_router_base_url(base_url)
    if not root_base:
        return []

    models_url = f"{root_base}/v1/models"
    unload_url = f"{root_base}/models/unload"
    headers = _auth_headers(api_key)

    async with httpx.AsyncClient(timeout=3.0) as client:
        try:
            resp = await client.get(models_url, headers=headers, timeout=3.0)
            resp.raise_for_status()
            payload = resp.json()
        except Exception as exc:
            logger.debug("router_memory: skipping capacity check (%s)", exc)
            return []

        data = payload.get("data") if isinstance(payload, Mapping) else None
        if not _is_llama_router_catalog(data):
            return []

        by_id: Dict[str, Mapping[str, Any]] = {
            str(item["id"]): item
            for item in data
            if isinstance(item, Mapping) and item.get("id")
        }
        resident = [
            model_id
            for model_id, item in by_id.items()
            if _resident_status_value(item) in {"loaded", "loading"}
        ]
        if clean_target in resident:
            touch_model(root_base, clean_target)
            return []

        footprints = {
            model_id: estimate_model_footprint_mib(model_id, by_id.get(model_id))
            for model_id in set(resident) | {clean_target}
        }
        current_mib = sum(footprints[m] for m in resident)
        target_mib = footprints[clean_target]
        if current_mib + target_mib <= budget_mib:
            touch_model(root_base, clean_target)
            return []

        last_used_map = _LAST_USED_BY_BASE.setdefault(root_base, {})
        candidates = [m for m in resident if m != clean_target]
        candidates.sort(key=lambda m: (last_used_map.get(m, 0.0), -footprints[m]))

        victims: List[str] = []
        projected_mib = current_mib
        freed_mib = 0
        for candidate in candidates:
            if projected_mib + target_mib <= budget_mib:
                break
            victims.append(candidate)
            projected_mib -= footprints[candidate]
            freed_mib += footprints[candidate]

        if not victims:
            touch_model(root_base, clean_target)
            return []

        evicted: List[str] = []
        for victim in victims:
            try:
                unload_resp = await client.post(
                    unload_url,
                    json={"model": victim},
                    headers=headers,
                    timeout=10.0,
                )
                if unload_resp.status_code in (200, 400):
                    evicted.append(victim)
                else:
                    logger.warning(
                        "router_memory: POST %s for %s returned %s: %s",
                        unload_url,
                        victim,
                        unload_resp.status_code,
                        unload_resp.text[:200],
                    )
            except Exception as exc:
                logger.warning(
                    "router_memory: failed to unload %s via %s: %s",
                    victim,
                    unload_url,
                    exc,
                )

        if evicted:
            await _wait_for_victims_unloaded(client, models_url, headers, evicted)
            for victim in evicted:
                last_used_map.pop(victim, None)
            logger.info(
                "router_memory: unloaded LRU models=%s (freed ~%d MiB) before loading %s (~%d MiB, projected=%d/%d MiB)",
                evicted,
                freed_mib,
                clean_target,
                target_mib,
                projected_mib + target_mib,
                budget_mib,
            )

        touch_model(root_base, clean_target)
        return evicted
