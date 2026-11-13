"""Thin compatibility alias to `wahs_core.fhir_browser` (`pmanko/wahs-engine`)."""

import sys

import wahs_core.fhir_browser as _core_fb

sys.modules[__name__] = _core_fb
