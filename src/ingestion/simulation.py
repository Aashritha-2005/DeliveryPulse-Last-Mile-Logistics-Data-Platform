"""Simulate a clean, physically consistent last-mile delivery world (no defects).

All times are integer UTC epoch seconds. All randomness comes from the single
``numpy.random.Generator`` passed in and is consumed in a fixed order, so the same seed and
config always produce the same world. Defects are added afterwards by ``defects.py``.

Physical consistency: each driver handles one delivery at a time and travels in straight lines
from their current position, at a speed fixed per driver (at most 30 km/h), so every pair of
consecutive GPS pings of a driver implies a plausible speed.
"""

import math
from dataclasses import dataclass

import numpy as np

from src.ingestion.generator_config import (
    CITIES,
    CITY_ORDER_WEIGHTS,
    SECONDS_PER_DAY,
    VEHICLE_TYPES,
    City,
    GeneratorConfig,
    format_coordinate,
    format_reporting_date,
    format_utc,
)

EARTH_RADIUS_KM = 6371.0088
KM_PER_DEGREE_LAT = 110.574
KM_PER_DEGREE_LON_AT_EQUATOR = 111.320
ROUTE_FACTOR = 1.3  # road distance / straight-line distance

VEHICLE_WEIGHTS = (0.10, 0.30, 0.55, 0.05)  # same order as VEHICLE_TYPES
VEHICLE_SPEED_RANGE_KMH = {
    "bicycle": (10.0, 15.0),
    "scooter": (15.0, 25.0),
    "motorbike": (18.0, 30.0),
    "car": (15.0, 25.0),
}
ORDER_HOURS = tuple(range(8, 24))  # reporting-timezone hours in which orders are placed
ORDER_HOUR_WEIGHTS = (
    0.02,
    0.03,
    0.04,
    0.06,
    0.10,
    0.11,
    0.07,
    0.04,
    0.04,
    0.05,
    0.07,
    0.10,
    0.11,
    0.09,
    0.05,
    0.02,
)
SHIFT_START_HOURS = (8, 11, 14, 16)  # reporting-timezone hours
SHIFT_START_WEIGHTS = (0.20, 0.25, 0.25, 0.30)
SHIFT_SECONDS = 8 * 3600
BREAK_OFFSET_SECONDS = 4 * 3600
BREAK_SECONDS = 30 * 60
LAST_ASSIGNMENT_MARGIN_SECONDS = 15 * 60
WORK_DAY_PROBABILITY = 5 / 7
RATING_MISSING_PROBABILITY = 0.10
CANDIDATE_DRIVERS_TO_TRY = 8
NO_DRIVER_CANCEL_DELAY_SECONDS = 120
MIN_DELIVERY_LEG_SECONDS = 60

CANCEL_CAUSE_CUSTOMER = "customer"
CANCEL_CAUSE_NO_DRIVER = "no_driver"
REMOVED_BATCH = 0  # placement batch meaning "not written to any batch"

Point = tuple[float, float]
Row = tuple[str, ...]

_EVENT_TYPE_RANK = {"assigned": 0, "gps_ping": 1, "picked_up": 2, "delivered": 3}
_SHIFT_EVENT_RANK = {"online": 0, "break_start": 1, "break_end": 2, "offline": 3}


@dataclass(frozen=True)
class DriverPlan:
    """Drivers reserved before simulation for driver-level defects.

    ``idle_driver_indices`` never work (no shifts, no orders); ``delayed_driver_windows`` maps a
    driver index to the batch window in which that driver first works.
    """

    idle_driver_indices: tuple[int, ...]
    delayed_driver_windows: tuple[tuple[int, int], ...]


@dataclass(slots=True)
class Driver:
    """A driver and their mutable dispatch state."""

    index: int
    driver_id: str
    city_index: int
    vehicle_type: str
    speed_kmh: float
    rating: float | None
    joined_date: str
    home: Point
    idle: bool
    delayed: bool
    first_work_window: int
    orders_allowed_from: int
    position: Point
    position_shift: int = -1
    free_at: int = 0


@dataclass(slots=True)
class Shift:
    """One working shift of one driver."""

    shift_index: int
    driver_index: int
    start: int
    end: int
    break_start: int
    break_end: int
    last_activity: int


@dataclass(slots=True)
class Order:
    """An order, its planning draws and its simulated outcome."""

    order_id: str
    customer_id: str
    city_index: int
    created: int
    promised: int
    distance_km: float
    fare_amount: float
    restaurant: Point
    customer: Point
    customer_cancels: bool
    customer_cancel_delay: int
    assign_wait: int
    prep_seconds: int
    handover_seconds: int
    driver_index: int | None = None
    assigned: int | None = None
    pickup: int | None = None
    delivered: int | None = None
    cancelled: int | None = None
    cancel_cause: str | None = None


@dataclass(slots=True)
class DeliveryEvent:
    """A status or GPS event of a delivered order."""

    order_id: str
    driver_id: str
    event_type: str
    ts: int
    lat: float
    lon: float
    event_id: str = ""


@dataclass(slots=True)
class ShiftEvent:
    """An online / break_start / break_end / offline event of a shift."""

    driver_id: str
    event_type: str
    ts: int
    event_id: str = ""


@dataclass(frozen=True)
class Timeline:
    """Planned times and start position of one delivery."""

    start: Point
    assigned: int
    arrive: int
    pickup: int
    leg_end: int
    delivered: int


@dataclass
class World:
    """The complete clean simulation output."""

    drivers: list[Driver]
    orders: list[Order]
    delivery_events: list[DeliveryEvent]
    shift_events: list[ShiftEvent]


@dataclass(slots=True)
class Placed:
    """A formatted record and the batch it is written to (``REMOVED_BATCH`` = not written)."""

    batch: int
    row: Row


@dataclass
class Placements:
    """Formatted records of every source with their batch placement."""

    drivers: list[Placed]
    orders: list[Placed]
    delivery_events: list[Placed]
    shift_events: list[Placed]


# ------------------------------------------------------------------ geometry ---


def haversine_km(a: Point, b: Point) -> float:
    """Great-circle distance in kilometres."""
    lat1 = math.radians(a[0])
    lat2 = math.radians(b[0])
    dlat = lat2 - lat1
    dlon = math.radians(b[1] - a[1])
    h = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return 2 * EARTH_RADIUS_KM * math.asin(math.sqrt(h))


def offset_point(origin: Point, distance_km: float, bearing_rad: float) -> Point:
    """Point ``distance_km`` from ``origin`` in direction ``bearing_rad`` (local flat approx.)."""
    lat = origin[0] + distance_km * math.cos(bearing_rad) / KM_PER_DEGREE_LAT
    lon_scale = KM_PER_DEGREE_LON_AT_EQUATOR * math.cos(math.radians(origin[0]))
    lon = origin[1] + distance_km * math.sin(bearing_rad) / lon_scale
    return (lat, lon)


def _approx_km(a: Point, b: Point) -> float:
    """Fast equirectangular distance, used only to rank candidate drivers."""
    mean_lat = math.radians((a[0] + b[0]) / 2)
    dx = (b[1] - a[1]) * KM_PER_DEGREE_LON_AT_EQUATOR * math.cos(mean_lat)
    dy = (b[0] - a[0]) * KM_PER_DEGREE_LAT
    return math.hypot(dx, dy)


def _interpolate(a: Point, b: Point, fraction: float) -> Point:
    return (a[0] + (b[0] - a[0]) * fraction, a[1] + (b[1] - a[1]) * fraction)


def _point_in_city(city: City, radius_km: float, radius_draw: float, angle_draw: float) -> Point:
    """Uniform random point within ``radius_km`` of the city centre."""
    distance = radius_km * math.sqrt(radius_draw)
    return offset_point((city.lat, city.lon), distance, 2 * math.pi * angle_draw)


# ------------------------------------------------------------------- drivers ---


def driver_city_indices(n_drivers: int) -> list[int]:
    """City index of every driver, proportional to city order weights (no randomness)."""
    quotas = [int(weight * n_drivers) for weight in CITY_ORDER_WEIGHTS]
    for position in range(n_drivers - sum(quotas)):
        quotas[position % len(quotas)] += 1
    return [city for city, quota in enumerate(quotas) for _ in range(quota)]


def _create_drivers(
    config: GeneratorConfig, rng: np.random.Generator, plan: DriverPlan
) -> list[Driver]:
    n = config.n_drivers
    cities = driver_city_indices(n)
    idle = set(plan.idle_driver_indices)
    delayed = dict(plan.delayed_driver_windows)
    window_seconds = config.days_per_batch * SECONDS_PER_DAY
    window_start = config.window_start_epoch()

    vehicle_draws = rng.choice(len(VEHICLE_TYPES), size=n, p=VEHICLE_WEIGHTS)
    speed_draws = rng.random(n)
    rating_missing_draws = rng.random(n)
    rating_draws = rng.uniform(3.0, 5.0, n)
    home_radius_draws = rng.random(n)
    home_angle_draws = rng.random(n)
    joined_days_draws = rng.integers(30, 900, size=n)

    drivers: list[Driver] = []
    for index in range(n):
        vehicle = VEHICLE_TYPES[int(vehicle_draws[index])]
        low, high = VEHICLE_SPEED_RANGE_KMH[vehicle]
        missing_rating = float(rating_missing_draws[index]) < RATING_MISSING_PROBABILITY
        rating = None if missing_rating else round(float(rating_draws[index]), 2)
        home = _point_in_city(
            CITIES[cities[index]],
            config.city_radius_km,
            float(home_radius_draws[index]),
            float(home_angle_draws[index]),
        )
        first_window = delayed.get(index, 1)
        if index in delayed:
            joined = format_reporting_date(window_start + (first_window - 1) * window_seconds)
            allowed_from = window_start + first_window * window_seconds
        else:
            joined_offset = int(joined_days_draws[index]) * SECONDS_PER_DAY
            joined = format_reporting_date(window_start - joined_offset)
            allowed_from = 0
        drivers.append(
            Driver(
                index=index,
                driver_id=f"DRV-{index + 1:05d}",
                city_index=cities[index],
                vehicle_type=vehicle,
                speed_kmh=low + (high - low) * float(speed_draws[index]),
                rating=rating,
                joined_date=joined,
                home=home,
                idle=index in idle,
                delayed=index in delayed,
                first_work_window=first_window,
                orders_allowed_from=allowed_from,
                position=home,
            )
        )
    return drivers


def _create_shifts(
    config: GeneratorConfig, rng: np.random.Generator, drivers: list[Driver]
) -> tuple[list[Shift], dict[tuple[int, int], list[Shift]]]:
    """Shifts per driver per day; a delayed driver always works the first day of its window."""
    window_start = config.window_start_epoch()
    total_days = config.n_batches * config.days_per_batch
    shifts: list[Shift] = []
    by_city_day: dict[tuple[int, int], list[Shift]] = {}
    for day in range(total_days):
        window = day // config.days_per_batch + 1
        for driver in drivers:
            if driver.idle or window < driver.first_work_window:
                continue
            works = float(rng.random()) < WORK_DAY_PROBABILITY
            hour_index = int(rng.choice(len(SHIFT_START_HOURS), p=SHIFT_START_WEIGHTS))
            first_day = (driver.first_work_window - 1) * config.days_per_batch
            forced = driver.delayed and day == first_day
            if not (works or forced):
                continue
            start = window_start + day * SECONDS_PER_DAY + SHIFT_START_HOURS[hour_index] * 3600
            break_start = start + BREAK_OFFSET_SECONDS
            shift = Shift(
                shift_index=len(shifts),
                driver_index=driver.index,
                start=start,
                end=start + SHIFT_SECONDS,
                break_start=break_start,
                break_end=break_start + BREAK_SECONDS,
                last_activity=start,
            )
            shifts.append(shift)
            by_city_day.setdefault((driver.city_index, day), []).append(shift)
    return shifts, by_city_day


# -------------------------------------------------------------------- orders ---


def _create_orders(config: GeneratorConfig, rng: np.random.Generator) -> list[Order]:
    """Orders sorted by creation time, with every per-order random draw made up front."""
    n = config.n_orders
    total_days = config.n_batches * config.days_per_batch
    window_start = config.window_start_epoch()

    days = rng.integers(0, total_days, size=n)
    hours = rng.choice(len(ORDER_HOURS), size=n, p=ORDER_HOUR_WEIGHTS)
    minutes = rng.integers(0, 60, size=n)
    seconds = rng.integers(0, 60, size=n)
    city_draws = rng.choice(len(CITIES), size=n, p=CITY_ORDER_WEIGHTS)
    restaurant_radius = rng.random(n)
    restaurant_angle = rng.random(n)
    customer_distance = rng.uniform(0.8, 6.0, n)
    customer_angle = rng.random(n)
    customer_numbers = rng.integers(1, max(1, n // 3) + 1, size=n)
    promise_noise = rng.integers(-300, 601, size=n)
    fare_noise = rng.uniform(-5.0, 15.0, n)
    cancel_draws = rng.random(n)
    cancel_delays = rng.integers(60, 241, size=n)
    assign_waits = rng.integers(30, 181, size=n)
    prep_times = rng.integers(8 * 60, 18 * 60 + 1, size=n)
    handover_times = rng.integers(60, 181, size=n)

    created_all = [
        window_start
        + int(days[i]) * SECONDS_PER_DAY
        + ORDER_HOURS[int(hours[i])] * 3600
        + int(minutes[i]) * 60
        + int(seconds[i])
        for i in range(n)
    ]
    sequence = sorted(range(n), key=lambda i: (created_all[i], i))

    orders: list[Order] = []
    for position, i in enumerate(sequence, start=1):
        city_index = int(city_draws[i])
        city = CITIES[city_index]
        restaurant = _point_in_city(
            city, config.city_radius_km, float(restaurant_radius[i]), float(restaurant_angle[i])
        )
        customer = offset_point(
            restaurant, float(customer_distance[i]), 2 * math.pi * float(customer_angle[i])
        )
        distance_km = round(haversine_km(restaurant, customer) * ROUTE_FACTOR, 3)
        estimate = 600 + 780 + distance_km / 20.0 * 3600 + int(promise_noise[i])
        promise_seconds = max(1200, math.ceil(estimate / 60) * 60)
        created = created_all[i]
        orders.append(
            Order(
                order_id=f"ORD-{position:07d}",
                customer_id=f"CUST-{int(customer_numbers[i]):06d}",
                city_index=city_index,
                created=created,
                promised=created + promise_seconds,
                distance_km=distance_km,
                fare_amount=round(25.0 + 10.0 * distance_km + float(fare_noise[i]), 2),
                restaurant=restaurant,
                customer=customer,
                customer_cancels=float(cancel_draws[i]) < config.customer_cancel_rate,
                customer_cancel_delay=int(cancel_delays[i]),
                assign_wait=int(assign_waits[i]),
                prep_seconds=int(prep_times[i]),
                handover_seconds=int(handover_times[i]),
            )
        )
    return orders


# ------------------------------------------------------------------ dispatch ---


def _timeline(driver: Driver, shift: Shift, order: Order, assigned: int) -> Timeline:
    start = driver.position if driver.position_shift == shift.shift_index else driver.home
    to_restaurant_km = haversine_km(start, order.restaurant) * ROUTE_FACTOR
    arrive = assigned + round(to_restaurant_km / driver.speed_kmh * 3600)
    pickup = max(arrive, order.created + order.prep_seconds)
    leg_seconds = max(MIN_DELIVERY_LEG_SECONDS, round(order.distance_km / driver.speed_kmh * 3600))
    leg_end = pickup + leg_seconds
    return Timeline(
        start=start,
        assigned=assigned,
        arrive=arrive,
        pickup=pickup,
        leg_end=leg_end,
        delivered=leg_end + order.handover_seconds,
    )


def _choose_driver(
    order: Order, assigned: int, shifts_today: list[Shift], drivers: list[Driver]
) -> tuple[Shift, Driver, Timeline] | None:
    """Nearest free, online driver whose delivery would not overlap their break."""
    candidates: list[tuple[float, int, Shift]] = []
    for shift in shifts_today:
        driver = drivers[shift.driver_index]
        if not shift.start <= assigned < shift.end - LAST_ASSIGNMENT_MARGIN_SECONDS:
            continue
        if driver.free_at > assigned or assigned < driver.orders_allowed_from:
            continue
        here = driver.position if driver.position_shift == shift.shift_index else driver.home
        candidates.append((_approx_km(here, order.restaurant), driver.index, shift))
    candidates.sort(key=lambda candidate: (candidate[0], candidate[1]))
    for _, driver_index, shift in candidates[:CANDIDATE_DRIVERS_TO_TRY]:
        driver = drivers[driver_index]
        timeline = _timeline(driver, shift, order, assigned)
        if assigned < shift.break_end and timeline.delivered > shift.break_start:
            continue
        return shift, driver, timeline
    return None


def _position_at(order: Order, timeline: Timeline, ts: int) -> Point:
    if ts <= timeline.arrive:
        span = timeline.arrive - timeline.assigned
        fraction = 1.0 if span == 0 else (ts - timeline.assigned) / span
        return _interpolate(timeline.start, order.restaurant, fraction)
    if ts <= timeline.pickup:
        return order.restaurant
    if ts <= timeline.leg_end:
        fraction = (ts - timeline.pickup) / (timeline.leg_end - timeline.pickup)
        return _interpolate(order.restaurant, order.customer, fraction)
    return order.customer


def _delivery_events(
    order: Order, driver: Driver, timeline: Timeline, ping_interval: int
) -> list[DeliveryEvent]:
    events = [
        DeliveryEvent(
            order.order_id,
            driver.driver_id,
            "assigned",
            timeline.assigned,
            timeline.start[0],
            timeline.start[1],
        )
    ]
    ts = timeline.assigned + ping_interval
    while ts < timeline.delivered:
        lat, lon = _position_at(order, timeline, ts)
        events.append(DeliveryEvent(order.order_id, driver.driver_id, "gps_ping", ts, lat, lon))
        ts += ping_interval
    restaurant_lat, restaurant_lon = order.restaurant
    customer_lat, customer_lon = order.customer
    events.append(
        DeliveryEvent(
            order.order_id,
            driver.driver_id,
            "picked_up",
            timeline.pickup,
            restaurant_lat,
            restaurant_lon,
        )
    )
    events.append(
        DeliveryEvent(
            order.order_id,
            driver.driver_id,
            "delivered",
            timeline.delivered,
            customer_lat,
            customer_lon,
        )
    )
    return events


def _dispatch(
    config: GeneratorConfig,
    orders: list[Order],
    drivers: list[Driver],
    shifts_by_city_day: dict[tuple[int, int], list[Shift]],
) -> list[DeliveryEvent]:
    """Assign every order (in creation order) or cancel it; return all delivery events."""
    window_start = config.window_start_epoch()
    events: list[DeliveryEvent] = []
    for order in orders:
        if order.customer_cancels:
            order.cancelled = order.created + order.customer_cancel_delay
            order.cancel_cause = CANCEL_CAUSE_CUSTOMER
            continue
        assigned = order.created + order.assign_wait
        day = (assigned - window_start) // SECONDS_PER_DAY
        shifts_today = shifts_by_city_day.get((order.city_index, day), [])
        choice = _choose_driver(order, assigned, shifts_today, drivers)
        if choice is None:
            order.cancelled = assigned + NO_DRIVER_CANCEL_DELAY_SECONDS
            order.cancel_cause = CANCEL_CAUSE_NO_DRIVER
            continue
        shift, driver, timeline = choice
        order.driver_index = driver.index
        order.assigned = timeline.assigned
        order.pickup = timeline.pickup
        order.delivered = timeline.delivered
        events.extend(_delivery_events(order, driver, timeline, config.gps_ping_interval_seconds))
        driver.position = order.customer
        driver.position_shift = shift.shift_index
        driver.free_at = timeline.delivered
        shift.last_activity = max(shift.last_activity, timeline.delivered)
    return events


def _shift_events(shifts: list[Shift], drivers: list[Driver]) -> list[ShiftEvent]:
    events: list[ShiftEvent] = []
    for shift in shifts:
        driver_id = drivers[shift.driver_index].driver_id
        events.append(ShiftEvent(driver_id, "online", shift.start))
        events.append(ShiftEvent(driver_id, "break_start", shift.break_start))
        events.append(ShiftEvent(driver_id, "break_end", shift.break_end))
        events.append(ShiftEvent(driver_id, "offline", max(shift.end, shift.last_activity)))
    events.sort(key=lambda event: (event.ts, event.driver_id, _SHIFT_EVENT_RANK[event.event_type]))
    for number, event in enumerate(events, start=1):
        event.event_id = f"SHF-{number:07d}"
    return events


def simulate(config: GeneratorConfig, rng: np.random.Generator, plan: DriverPlan) -> World:
    """Run the whole clean simulation."""
    drivers = _create_drivers(config, rng, plan)
    shifts, shifts_by_city_day = _create_shifts(config, rng, drivers)
    orders = _create_orders(config, rng)
    delivery_events = _dispatch(config, orders, drivers, shifts_by_city_day)
    delivery_events.sort(
        key=lambda event: (event.order_id, event.ts, _EVENT_TYPE_RANK[event.event_type])
    )
    for number, event in enumerate(delivery_events, start=1):
        event.event_id = f"EVT-{number:09d}"
    return World(
        drivers=drivers,
        orders=orders,
        delivery_events=delivery_events,
        shift_events=_shift_events(shifts, drivers),
    )


# ----------------------------------------------------------------- placement ---


def batch_for(ts: int, cutoffs: tuple[int, ...]) -> int | None:
    """First batch (1-based) whose cutoff is after ``ts``; None if after the last cutoff."""
    for sequence, cutoff in enumerate(cutoffs, start=1):
        if ts < cutoff:
            return sequence
    return None


def order_transitions(order: Order) -> list[tuple[int, str]]:
    """Status changes of an order as (timestamp, status), oldest first."""
    transitions = [(order.created, "created")]
    if order.assigned is not None:
        transitions.append((order.assigned, "assigned"))
    if order.pickup is not None:
        transitions.append((order.pickup, "picked_up"))
    if order.delivered is not None:
        transitions.append((order.delivered, "delivered"))
    if order.cancelled is not None:
        transitions.append((order.cancelled, "cancelled"))
    return transitions


def order_row(order: Order, updated_at: int, status: str, drivers: list[Driver]) -> Row:
    """Format the version of ``order`` that has ``status`` as of ``updated_at``."""
    driver_id = ""
    if status in ("assigned", "picked_up", "delivered") and order.driver_index is not None:
        driver_id = drivers[order.driver_index].driver_id
    pickup = ""
    if status in ("picked_up", "delivered") and order.pickup is not None:
        pickup = format_utc(order.pickup)
    delivery = ""
    if status == "delivered" and order.delivered is not None:
        delivery = format_utc(order.delivered)
    return (
        order.order_id,
        order.customer_id,
        driver_id,
        format_utc(order.created),
        pickup,
        delivery,
        format_utc(order.promised),
        f"{order.distance_km:.3f}",
        status,
        CITIES[order.city_index].name,
        f"{order.fare_amount:.2f}",
        format_utc(updated_at),
    )


def _order_placements(
    order: Order, drivers: list[Driver], cutoffs: tuple[int, ...]
) -> list[Placed]:
    """One placement per batch in which the order's state changed (change-data capture)."""
    transitions = order_transitions(order)
    placed: list[Placed] = []
    previous: tuple[int, str] | None = None
    for sequence, cutoff in enumerate(cutoffs, start=1):
        state: tuple[int, str] | None = None
        for transition in transitions:
            if transition[0] < cutoff:
                state = transition
        if state is not None and state != previous:
            placed.append(Placed(sequence, order_row(order, state[0], state[1], drivers)))
            previous = state
    return placed


def place_records(config: GeneratorConfig, world: World) -> Placements:
    """Format every record and place it in the batch where it first becomes available."""
    cutoffs = config.cutoffs()
    drivers = [
        Placed(
            1,
            (
                driver.driver_id,
                CITIES[driver.city_index].name,
                driver.vehicle_type,
                driver.joined_date,
                "" if driver.rating is None else f"{driver.rating:.2f}",
            ),
        )
        for driver in world.drivers
    ]
    orders = [
        placed
        for order in world.orders
        for placed in _order_placements(order, world.drivers, cutoffs)
    ]
    delivery_events: list[Placed] = []
    for event in world.delivery_events:
        sequence = batch_for(event.ts, cutoffs)
        if sequence is not None:
            row = (
                event.event_id,
                event.order_id,
                event.driver_id,
                event.event_type,
                format_utc(event.ts),
                format_coordinate(event.lat),
                format_coordinate(event.lon),
            )
            delivery_events.append(Placed(sequence, row))
    shift_events: list[Placed] = []
    for shift_event in world.shift_events:
        sequence = batch_for(shift_event.ts, cutoffs)
        if sequence is not None:
            shift_row = (
                shift_event.event_id,
                shift_event.driver_id,
                shift_event.event_type,
                format_utc(shift_event.ts),
            )
            shift_events.append(Placed(sequence, shift_row))
    return Placements(drivers, orders, delivery_events, shift_events)
