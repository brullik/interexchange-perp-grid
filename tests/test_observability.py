from __future__ import annotations

import json
import logging

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


def test_standard_logging_redacts_telegram_token_and_suppresses_httpx_info(
    capsys: object,
) -> None:
    configure_logging("INFO")
    token = f"{123_456_789}:{'A' * 33}"
    logging.getLogger("third_party").warning(
        "POST https://api.telegram.org/bot%s/getUpdates", token
    )
    logging.getLogger("third_party").error('credential={"token":"%s"}', token)
    structlog.get_logger().warning("raw_credential", credential=token)
    logging.getLogger("httpx").info("HTTP Request with %s", token)

    output = capsys.readouterr().out  # type: ignore[attr-defined]
    assert token not in output
    assert output.count("<REDACTED_TELEGRAM_TOKEN>") == 3
    assert "HTTP Request" not in output
