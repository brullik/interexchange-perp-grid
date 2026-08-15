from hypothesis import given
from hypothesis import strategies as st

from interexchange_perp_grid.client_ids import (
    BOT_CLIENT_ID_PREFIX,
    is_bot_client_order_id,
    parse_bot_client_order_id,
    venue_client_order_id,
)


@given(
    action_id=st.text(alphabet=st.characters(categories=("L", "N")), min_size=1, max_size=100),
    role=st.text(
        alphabet="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789",
        min_size=1,
        max_size=30,
    ),
    sequence=st.integers(min_value=0, max_value=2**31 - 1),
)
def test_generated_client_id_always_satisfies_shared_wave1_contract(
    action_id: str,
    role: str,
    sequence: int,
) -> None:
    value = venue_client_order_id(action_id, role, sequence)
    parsed = parse_bot_client_order_id(value)
    assert len(value) == 32
    assert value.isascii() and value.isalnum()
    assert value.startswith(BOT_CLIENT_ID_PREFIX)
    assert parsed is not None
    assert is_bot_client_order_id(value)


def test_client_id_namespace_rejects_external_lookalikes_and_bad_checksum() -> None:
    generated = venue_client_order_id("action", "open", 1)
    replacement = "0" if generated[-1] != "0" else "1"
    assert not is_bot_client_order_id(f"x{generated[1:]}")
    assert not is_bot_client_order_id(f"{generated[:-1]}{replacement}")
    assert not is_bot_client_order_id("ipeg-open-order")
