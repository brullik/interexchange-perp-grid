from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
EXCLUDED_PARTS = {
    ".git",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    ".venv",
    "__pycache__",
    "artifacts",
    "data",
    "logs",
    "state",
}


def repository_files() -> list[Path]:
    return [
        path
        for path in ROOT.rglob("*")
        if path.is_file() and not (EXCLUDED_PARTS & set(path.relative_to(ROOT).parts))
    ]


def test_fast_track_contract_is_lean() -> None:
    required = {
        "AGENTS.md",
        "GOAL.md",
        "FAST_TRACK_PLAN.md",
        "ACCEPTANCE.md",
        "STATUS.md",
        "CODEX_START_PROMPT_RU.md",
    }
    assert required <= {path.name for path in ROOT.iterdir() if path.is_file()}

    markdown_files = [path for path in repository_files() if path.suffix == ".md"]
    assert len(markdown_files) <= 12, "do not recreate documentation sprawl"


def test_no_runtime_secret_or_database_is_committed() -> None:
    forbidden_names = {".env", "state.sqlite3", "ipeg.sqlite3"}
    committed_names = {path.name for path in repository_files()}
    assert not (forbidden_names & committed_names)


def test_no_withdrawal_or_transfer_implementation_exists() -> None:
    source = "\n".join(
        path.read_text(encoding="utf-8") for path in (ROOT / "src").rglob("*.py")
    ).lower()
    forbidden_callable_tokens = (
        "withdraw(",
        ".withdraw(",
        "transfer_funds(",
        "create_withdrawal(",
    )
    assert all(token not in source for token in forbidden_callable_tokens)
