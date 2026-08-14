from __future__ import annotations

import json

import structlog

from interexchange_perp_grid.observability import configure_logging, render_metrics


def test_structured_logging_and_metrics_skeleton(capsys: object) -> None:
    configure_logging("INFO")
    structlog.get_logger().info("test_event", reason_code="TEST_REASON")
    output = capsys.readouterr().out  # type: ignore[attr-defined]
    payload = json.loads(output)
    assert payload["event"] == "test_event"
    assert payload["reason_code"] == "TEST_REASON"
    metrics = render_metrics()
    assert "ipeg_service_up" in metrics
    assert "ipeg_live_guard_denials_total" in metrics
