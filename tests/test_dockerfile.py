from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_runtime_image_has_no_package_mirror_dependency():
    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
    runtime = dockerfile.split("# Stage 2: runtime", 1)[1]

    assert "apt-get" not in runtime
    assert "curl" not in runtime
    assert "urllib.request.urlopen" in runtime
    assert "USER 65532:65532" in runtime
