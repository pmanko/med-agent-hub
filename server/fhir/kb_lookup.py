"""Thin compatibility alias to `wahs_core.kb_lookup` (`pmanko/wahs-engine`)."""

import sys

import wahs_core.kb_lookup as _core_kbl

sys.modules[__name__] = _core_kbl
