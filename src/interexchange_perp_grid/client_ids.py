from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass

BOT_CLIENT_ID_PREFIX = "ipeg1"
_BOT_CLIENT_ID = re.compile(
    rf"^{BOT_CLIENT_ID_PREFIX}(?P<role>[a-z0-9]{{3}})"
    r"(?P<digest>[0-9a-f]{20})(?P<checksum>[0-9a-f]{4})$"
)


@dataclass(frozen=True, slots=True)
class BotClientOrderId:
    value: str
    role_code: str
    identity_digest: str


def venue_client_order_id(action_id: str, role: str, sequence: int = 0) -> str:
    """Return a self-validating Wave-1-compatible alphanumeric ID of exactly 32 chars."""
    if not action_id.strip() or sequence < 0:
        raise ValueError("client order ID action and sequence are invalid")
    normalized_role = "".join(
        character for character in role.lower() if character.isascii() and character.isalnum()
    )
    if not normalized_role:
        raise ValueError("client order ID role must contain an alphanumeric character")
    role_code = normalized_role[:3].ljust(3, "x")
    identity = hashlib.sha256(f"{action_id}:{role}:{sequence}".encode()).hexdigest()[:20]
    body = f"{BOT_CLIENT_ID_PREFIX}{role_code}{identity}"
    checksum = hashlib.sha256(body.encode()).hexdigest()[:4]
    return f"{body}{checksum}"


def parse_bot_client_order_id(value: str) -> BotClientOrderId | None:
    matched = _BOT_CLIENT_ID.fullmatch(value)
    if matched is None:
        return None
    body = value[:-4]
    expected_checksum = hashlib.sha256(body.encode()).hexdigest()[:4]
    if matched.group("checksum") != expected_checksum:
        return None
    return BotClientOrderId(
        value=value,
        role_code=matched.group("role"),
        identity_digest=matched.group("digest"),
    )


def is_bot_client_order_id(value: str) -> bool:
    return parse_bot_client_order_id(value) is not None
