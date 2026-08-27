from __future__ import annotations

import base64
import hashlib

from interexchange_perp_grid.canary_runtime import local_live_unlock_valid


def _environment(secret: str) -> dict[str, str]:
    salt = bytes(range(32))
    digest = hashlib.pbkdf2_hmac("sha256", secret.encode(), salt, 600_000, dklen=32)
    return {
        "IPEG_LOCAL_UNLOCK_SECRET": secret,
        "IPEG_LOCAL_UNLOCK_VERIFIER": "pbkdf2-sha256$600000$"
        + base64.b64encode(salt).decode()
        + "$"
        + base64.b64encode(digest).decode(),
    }


def test_local_unlock_requires_matching_pbkdf2_verifier() -> None:
    assert local_live_unlock_valid(_environment("correct horse battery staple"))
    assert not local_live_unlock_valid(
        {**_environment("correct horse battery staple"), "IPEG_LOCAL_UNLOCK_SECRET": "wrong"}
    )


def test_local_unlock_rejects_missing_malformed_and_short_values() -> None:
    assert not local_live_unlock_valid({})
    assert not local_live_unlock_valid(
        {"IPEG_LOCAL_UNLOCK_SECRET": "x" * 16, "IPEG_LOCAL_UNLOCK_VERIFIER": "bad"}
    )
    assert not local_live_unlock_valid(_environment("too-short"))
