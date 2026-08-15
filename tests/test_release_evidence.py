from pathlib import Path

import pytest

from interexchange_perp_grid.release_evidence import _junit_counts


def test_junit_evidence_counts_are_read_from_the_test_runner_artifact(tmp_path: Path) -> None:
    report = tmp_path / "report.xml"
    report.write_text(
        '<testsuites tests="27" failures="0" errors="0" skipped="0"></testsuites>',
        encoding="utf-8",
    )
    assert _junit_counts(report) == (27, 0, 0, 0)


def test_unexpected_junit_payload_is_rejected(tmp_path: Path) -> None:
    report = tmp_path / "report.xml"
    report.write_text("<not-tests />", encoding="utf-8")
    with pytest.raises(ValueError, match="JUnit"):
        _junit_counts(report)
