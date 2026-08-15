from __future__ import annotations

from decimal import Decimal

from hypothesis import given
from hypothesis import strategies as st

from interexchange_perp_grid.domain import Venue
from interexchange_perp_grid.reason_codes import ReasonCode
from interexchange_perp_grid.risk import (
    RiskBook,
    RiskLimits,
    RiskRequest,
    VenueProjection,
)


def limits() -> RiskLimits:
    return RiskLimits(
        pair_stress_usdt=Decimal("5"),
        portfolio_stress_usdt=Decimal("50"),
        max_active_routes=10,
        max_routes_per_base=1,
        max_tranches_per_route=5,
        local_free_margin_floor_ratio=Decimal("0.20"),
        effective_leverage_cap=Decimal("3"),
    )


def healthy_venues() -> tuple[VenueProjection, ...]:
    return (
        VenueProjection(Venue.BYBIT, Decimal("100"), Decimal("100"), Decimal("20"), Decimal("2")),
        VenueProjection(Venue.OKX, Decimal("100"), Decimal("100"), Decimal("20"), Decimal("2")),
    )


def request(
    index: int,
    stress: Decimal,
    *,
    route: str = "BTC:bybit>okx",
    base: str = "BTC",
    venues: tuple[VenueProjection, ...] | None = None,
    depth: bool = True,
) -> RiskRequest:
    return RiskRequest(
        reservation_id=f"reservation-{index}",
        route_id=route,
        base=base,
        tranche_id=f"tranche-{index}",
        projected_stress_usdt=stress,
        venues=venues or healthy_venues(),
        exit_depth_sufficient=depth,
    )


@given(st.lists(st.integers(min_value=1, max_value=600), min_size=1, max_size=20))
def test_every_accepted_reservation_preserves_pair_and_portfolio_limits(
    cents: list[int],
) -> None:
    book = RiskBook(limits())
    for index, value in enumerate(cents):
        decision = book.reserve(request(index, Decimal(value) / 100))
        per_route, portfolio = book.totals()
        assert all(stress <= Decimal("5") for stress in per_route.values())
        assert portfolio <= Decimal("50")
        assert len(book.reservations) <= 5
        if decision.accepted:
            assert decision.reason == ReasonCode.RISK_RESERVED


def test_risk_enforces_route_base_tranche_margin_leverage_and_depth_limits() -> None:
    book = RiskBook(limits())
    assert book.reserve(request(0, Decimal("0.5"))).accepted
    same_base = book.reserve(request(1, Decimal("0.5"), route="BTC:okx>bybit", base="BTC"))
    assert same_base.reason == ReasonCode.BASE_ROUTE_LIMIT

    for index in range(1, 5):
        assert book.reserve(request(index, Decimal("0.5"))).accepted
    assert book.reserve(request(5, Decimal("0.5"))).reason == ReasonCode.TRANCHE_LIMIT

    low_margin = (
        VenueProjection(Venue.BYBIT, Decimal("100"), Decimal("100"), Decimal("75"), Decimal("6")),
        healthy_venues()[1],
    )
    margin_book = RiskBook(limits())
    assert (
        margin_book.reserve(request(10, Decimal("1"), venues=low_margin)).reason
        == ReasonCode.LOCAL_MARGIN_FLOOR
    )
    excessive_leverage = (
        VenueProjection(Venue.BYBIT, Decimal("100"), Decimal("301"), Decimal("20"), Decimal("2")),
        healthy_venues()[1],
    )
    assert (
        RiskBook(limits()).reserve(request(11, Decimal("1"), venues=excessive_leverage)).reason
        == ReasonCode.EFFECTIVE_LEVERAGE_LIMIT
    )
    assert (
        RiskBook(limits()).reserve(request(12, Decimal("1"), depth=False)).reason
        == ReasonCode.DEPTH_INSUFFICIENT
    )


def test_risk_blocks_unknown_or_unmatched_execution_and_reserves_atomically() -> None:
    book = RiskBook(limits())
    book.set_execution_block(unmatched_exposure=True, unknown_order_state=False)
    rejected = book.reserve(request(0, Decimal("1")))
    assert rejected.reason == ReasonCode.UNRESOLVED_EXECUTION_STATE
    assert book.reservations == ()

    book.set_execution_block(unmatched_exposure=False, unknown_order_state=False)
    accepted = book.reserve(request(0, Decimal("4")))
    assert accepted.accepted
    over_limit = book.reserve(request(1, Decimal("2")))
    assert over_limit.reason == ReasonCode.PAIR_STRESS_LIMIT
    per_route, portfolio = book.totals()
    assert per_route == {"BTC:bybit>okx": Decimal("4")}
    assert portfolio == Decimal("4")


def test_risk_caps_ten_routes_and_fifty_usdt_portfolio_stress() -> None:
    route_book = RiskBook(limits())
    for index in range(10):
        assert route_book.reserve(
            request(
                index,
                Decimal("0.1"),
                route=f"BASE{index}:bybit>okx",
                base=f"BASE{index}",
            )
        ).accepted
    route_limit = route_book.reserve(
        request(10, Decimal("0.1"), route="EXTRA:bybit>okx", base="EXTRA")
    )
    assert route_limit.reason == ReasonCode.ROUTE_LIMIT

    portfolio_book = RiskBook(limits())
    for index in range(10):
        assert portfolio_book.reserve(
            request(
                index,
                Decimal("5"),
                route=f"BASE{index}:bybit>okx",
                base=f"BASE{index}",
            )
        ).accepted
    portfolio_limit = portfolio_book.reserve(
        request(10, Decimal("0.01"), route="EXTRA:bybit>okx", base="EXTRA")
    )
    assert portfolio_limit.reason == ReasonCode.PORTFOLIO_STRESS_LIMIT
