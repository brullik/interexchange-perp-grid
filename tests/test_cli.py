from __future__ import annotations

import re

from typer.testing import CliRunner

from interexchange_perp_grid.cli import app

runner = CliRunner()
ANSI_ESCAPE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")


def test_cli_and_public_scan_help_render() -> None:
    assert runner.invoke(app, ["--help"]).exit_code == 0
    public_help = runner.invoke(app, ["public-scan", "--help"])
    assert public_help.exit_code == 0
    assert "--quantity" in ANSI_ESCAPE.sub("", public_help.output)


def test_public_scan_rejects_non_decimal_quantity_before_network() -> None:
    result = runner.invoke(app, ["public-scan", "--quantity", "not-a-number"])
    assert result.exit_code == 2
    assert "quantity must be a decimal number" in result.output
