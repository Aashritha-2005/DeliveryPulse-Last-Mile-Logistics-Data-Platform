"""Handmade dashboard results for unit tests (no database)."""

from datetime import date
from decimal import Decimal

from src.dashboard.data import (
    CityLateRate,
    DashboardData,
    DriverRow,
    Filters,
    HourCount,
    Kpis,
    TrendPoint,
)

FILTERS = Filters(date(2026, 9, 10), date(2026, 9, 11), ("Delhi", "Mumbai"))


def sample_data() -> DashboardData:
    """Two cities over two days; every view has rows."""
    return DashboardData(
        filters=FILTERS,
        # 40 created, 4 cancelled (10 %); 32 delivered, 8 late (25 %); 960 min in total.
        kpis=Kpis(40, 4, 32, 8, Decimal(57600)),
        trend=(
            TrendPoint(date(2026, 9, 10), "Delhi", Decimal("28.50"), 10),
            TrendPoint(date(2026, 9, 10), "Mumbai", Decimal("31.00"), 6),
            TrendPoint(date(2026, 9, 11), "Delhi", Decimal("30.25"), 9),
            TrendPoint(date(2026, 9, 11), "Mumbai", Decimal("32.00"), 7),
        ),
        late_by_city=(CityLateRate("Delhi", 19, 4), CityLateRate("Mumbai", 13, 4)),
        peak_hours=(HourCount(12, 15), HourCount(19, 25)),
        top_drivers=(
            DriverRow("DRV-00001", "Delhi", 6, Decimal("24.10"), Decimal("41.50")),
            DriverRow("DRV-00002", "Mumbai", 5, Decimal("26.00"), None),
        ),
    )


def empty_data() -> DashboardData:
    """Filters that match nothing."""
    return DashboardData(FILTERS, Kpis(0, 0, 0, 0, Decimal(0)), (), (), (), ())
