"""
File-backed system prompts for the Med Agent Team.

Each team prompt lives as a plain-text file under ``server/prompts/`` and is read
PER REQUEST, so editing a (bind-mounted) ``.txt`` changes behaviour with no
rebuild or restart. The files are the single source of truth; git is the version
history.

The names the team uses are ``orchestrator``, ``medical_expert``, ``synthesis``,
and ``synthesis-low`` (the low level swaps synthesis for the last via its
``TEAM_PRESET``). A name with no file is a configuration error, raised loudly
rather than silently substituted.

To trial an alternative prompt concurrently, add another file (e.g.
``synthesis-experimental.txt``) and point a ``TEAM_PRESET``/env override at it;
the committed file stays the default. Config + git, no runtime variant overlay.
"""

from pathlib import Path

_DIR = Path(__file__).parent / "prompts"


def load_prompt(name: str) -> str:
    """Return the text of prompt ``name`` from ``prompts/<name>.txt``.

    Read fresh on every call so editing a mounted ``.txt`` takes effect with no
    restart. A missing file raises ``FileNotFoundError`` naming the path: the
    prompt files are the single source of truth, so a referenced-but-absent
    prompt is a configuration bug, not something to silently paper over. The
    trailing newline the files carry for tidiness is stripped.
    """
    path = _DIR / f"{name}.txt"
    try:
        return path.read_text(encoding="utf-8").rstrip("\n")
    except FileNotFoundError as exc:
        raise FileNotFoundError(
            f"prompt {name!r} not found at {path} — every prompt a TEAM_PRESET "
            f"references must have a file in {_DIR}"
        ) from exc
