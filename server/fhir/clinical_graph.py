"""Thin compatibility alias to `wahs_core.clinical_graph` (`pmanko/wahs-engine`)."""

import sys

import wahs_core.clinical_graph as _core_cg

from .hub_catalog_bridge import ensure_hub_catalog_registered

ensure_hub_catalog_registered()
sys.modules[__name__] = _core_cg
