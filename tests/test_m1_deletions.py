from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_dead_a2a_and_mcp_runtime_is_deleted():
    removed = [
        "launch_a2a_agents.py",
        "explore_a2a.py",
        "server/sdk_agents",
        "server/agent_configs",
        "server/mcp",
        "server/llm_clients.py",
        "server/schemas.py",
    ]

    assert [path for path in removed if (ROOT / path).exists()] == []


def test_runtime_dependencies_and_config_have_no_a2a_residue():
    pyproject = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    config = (ROOT / "server/config.py").read_text(encoding="utf-8")
    env = (ROOT / "env.recommended").read_text(encoding="utf-8")

    assert "a2a-sdk" not in pyproject
    assert "A2A_" not in config
    assert "A2A_" not in env


def test_old_topology_flag_matrix_is_absent_from_active_profile_runtime():
    active = "\n".join(
        (ROOT / path).read_text(encoding="utf-8")
        for path in ("server/levels_loader.py", "server/levels.yaml", "server/team.py")
    )
    legacy_field = re.compile(
        r"\b(two_call|indepth_shared|indepth_only|answer_only|answer_review|solo)\s*[:=]"
    )
    assert legacy_field.findall(active) == []
    assert not (ROOT / "tests/profile_runner.py").exists()
    factories = (ROOT / "tests/factories.py").read_text(encoding="utf-8")
    assert legacy_field.findall(factories) == []
