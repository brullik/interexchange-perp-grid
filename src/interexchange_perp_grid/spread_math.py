from __future__ import annotations

from decimal import ROUND_HALF_EVEN, Decimal, localcontext

_BPS_SCALE = Decimal("10000")
_BPS_QUANTUM = Decimal("0.00000001")


def log_ratio_bps(numerator: Decimal, denominator: Decimal) -> Decimal:
    """Return one canonical fixed-precision log price ratio in basis points."""
    if any(not value.is_finite() or value <= 0 for value in (numerator, denominator)):
        raise ValueError("spread prices must be positive and finite")
    with localcontext() as context:
        context.prec = 50
        value = (numerator / denominator).ln() * _BPS_SCALE
        return value.quantize(_BPS_QUANTUM, rounding=ROUND_HALF_EVEN)
