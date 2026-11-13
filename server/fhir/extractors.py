"""Thin compatibility alias to `wahs_core.extractors` (`pmanko/wahs-engine`)."""

import sys

import wahs_core.extractors as _core_ext

sys.modules[__name__] = _core_ext
