"""Inject documented defects into placed records and describe each one as a manifest case.

Rules that keep later reconciliation exact:

* Each scenario uses its own order (or driver), drawn uniformly at random without replacement
  from the orders still eligible for its type, so no record, order or driver carries two
  injected defects and no batch is favoured.
* Order-level defects use orders with exactly one version, delivered within one batch window.
* Orders whose only row is rejected get no delivery events, so the rejection never creates
  orphan events as a side effect.
* Driver-level defects use drivers reserved before simulation: invalid drivers never work and
  delayed drivers get no orders in the window before their driver row arrives.
"""

import math
from collections.abc import Callable
from dataclasses import dataclass

import numpy as np

from src.groundtruth.manifest import ACCEPTED, DUPLICATE, PENDING, REJECTED, STALE_IGNORED, Case
from src.ingestion.generator_config import (
    CITIES,
    DELIVERY_EVENT_FIELDS,
    DRIVER_COLUMNS,
    MIN_WORKING_DRIVERS_PER_CITY,
    ORDER_COLUMNS,
    SHIFT_EVENT_COLUMNS,
    GenerationError,
    GeneratorConfig,
    format_coordinate,
    format_utc,
    parse_utc,
)
from src.ingestion.simulation import (
    REMOVED_BATCH,
    DriverPlan,
    Order,
    Placed,
    Placements,
    Row,
    World,
    driver_city_indices,
    offset_point,
    order_row,
)

O_CUSTOMER = ORDER_COLUMNS.index("customer_id")
O_PICKUP = ORDER_COLUMNS.index("pickup_ts")
O_DELIVERY = ORDER_COLUMNS.index("delivery_ts")
O_PROMISED = ORDER_COLUMNS.index("promised_ts")
O_DISTANCE = ORDER_COLUMNS.index("distance_km")
O_STATUS = ORDER_COLUMNS.index("status")
O_CITY = ORDER_COLUMNS.index("city")
O_FARE = ORDER_COLUMNS.index("fare_amount")
E_ID = DELIVERY_EVENT_FIELDS.index("event_id")
E_TYPE = DELIVERY_EVENT_FIELDS.index("event_type")
E_TS = DELIVERY_EVENT_FIELDS.index("event_ts")
E_LAT = DELIVERY_EVENT_FIELDS.index("lat")
E_LON = DELIVERY_EVENT_FIELDS.index("lon")
D_VEHICLE = DRIVER_COLUMNS.index("vehicle_type")
D_RATING = DRIVER_COLUMNS.index("rating")
S_DRIVER = SHIFT_EVENT_COLUMNS.index("driver_id")

MISSING_VALUE_FIELDS = (O_CUSTOMER, O_CITY, O_PROMISED)
INVALID_STATUSES = ("in_transit", "lost", "unknown")
UNNORMALISED_DELIVERED = (" delivered", "Delivered", "DELIVERED", "delivered ")
UNPARSEABLE_FIELDS = (O_PICKUP, O_DELIVERY, O_PROMISED)
UNPARSEABLE_VALUES = ("2026-02-30T25:61:00Z", "yesterday", "30/09/2026 18:30")
INVALID_DRIVER_VALUES = (
    (D_RATING, "5.70", "out_of_range_value"),
    (D_RATING, "0.50", "out_of_range_value"),
    (D_VEHICLE, "truck", "invalid_vehicle_type"),
    (D_VEHICLE, "drone", "invalid_vehicle_type"),
)
IMPOSSIBLE_JUMP_KM = (30.0, 80.0)
DUPLICATE_TIMESTAMP_OFFSET_KM = 0.2
LONG_GAP_REMOVED_PINGS = 4

SOURCE_DRIVERS = "drivers"
SOURCE_ORDERS = "orders"
SOURCE_EVENTS = "delivery_events"
SOURCE_SHIFTS = "shift_events"


def _with(row: Row, index: int, value: str) -> Row:
    """Copy of ``row`` with field ``index`` replaced."""
    return row[:index] + (value,) + row[index + 1 :]


def plan_drivers(config: GeneratorConfig, rng: np.random.Generator) -> DriverPlan:
    """Reserve drivers for driver-level defects, keeping enough working drivers per city."""
    cities = driver_city_indices(config.n_drivers)
    working = [cities.count(city) for city in range(len(CITIES))]
    wanted_idle = config.defect_count("invalid_driver_value")
    wanted_delayed = config.defect_count("delayed_driver_record")
    idle: list[int] = []
    delayed: list[tuple[int, int]] = []
    for draw in rng.permutation(config.n_drivers):
        if len(idle) == wanted_idle and len(delayed) == wanted_delayed:
            break
        index = int(draw)
        if working[cities[index]] <= MIN_WORKING_DRIVERS_PER_CITY:
            continue
        working[cities[index]] -= 1
        if len(idle) < wanted_idle:
            idle.append(index)
        else:
            delayed.append((index, int(rng.integers(1, config.n_batches))))
    if len(idle) < wanted_idle or len(delayed) < wanted_delayed:
        raise GenerationError("Not enough drivers for driver-level defects; increase --drivers.")
    return DriverPlan(tuple(sorted(idle)), tuple(sorted(delayed)))


@dataclass(frozen=True)
class _Candidate:
    """A delivered order with exactly one version, eligible for one scenario."""

    order_id: str
    batch: int
    order_index: int
    event_indices: tuple[int, ...]
    ping_indices: tuple[int, ...]


class _OrderPicker:
    """Draws distinct orders uniformly at random from the eligible, still-unused pool.

    On the first draw for a defect type, every candidate that is eligible for that type and not
    yet used by an earlier type is shuffled with the shared generator; draws for the type then
    walk that shuffle. Each type therefore gets a uniform sample without replacement of its own
    eligible pool, independent of which candidates earlier types consumed.
    """

    def __init__(self, candidates: list[_Candidate], rng: np.random.Generator) -> None:
        self._candidates = candidates
        self._rng = rng
        self._used: set[str] = set()
        self._queues: dict[str, list[_Candidate]] = {}
        self._positions: dict[str, int] = {}

    def take(self, defect_type: str, eligible: Callable[[_Candidate], bool]) -> _Candidate:
        queue = self._queues.get(defect_type)
        if queue is None:
            pool = [c for c in self._candidates if c.order_id not in self._used and eligible(c)]
            queue = [pool[int(i)] for i in self._rng.permutation(len(pool))]
            self._queues[defect_type] = queue
        position = self._positions.get(defect_type, 0)
        while position < len(queue):
            candidate = queue[position]
            position += 1
            if candidate.order_id not in self._used:
                self._used.add(candidate.order_id)
                self._positions[defect_type] = position
                return candidate
        raise GenerationError(f"Not enough eligible orders for {defect_type}; increase --orders.")


class _Injector:
    """Holds the shared state of one injection run."""

    def __init__(
        self,
        config: GeneratorConfig,
        rng: np.random.Generator,
        world: World,
        placements: Placements,
    ) -> None:
        self.config = config
        self.rng = rng
        self.world = world
        self.p = placements
        self.cases: list[Case] = []
        self._scenario_numbers: dict[str, int] = {}
        self._orders_by_id: dict[str, Order] = {order.order_id: order for order in world.orders}
        self._next_event_number = len(world.delivery_events) + 1

    # ----------------------------------------------------------------- helpers ---

    def scenario(self, defect_type: str) -> str:
        number = self._scenario_numbers.get(defect_type, 0) + 1
        self._scenario_numbers[defect_type] = number
        return f"{defect_type}-{number:05d}"

    def add(
        self,
        scenario: str,
        defect_type: str,
        source: str,
        batch: int,
        record_key: str,
        disposition: str,
        reason: str = "",
        related: tuple[str, ...] = (),
        injected: bool = True,
    ) -> None:
        self.cases.append(
            Case(
                scenario=scenario,
                defect_type=defect_type,
                source=source,
                batch=batch,
                record_key=record_key,
                expected_disposition=disposition,
                reason=reason,
                related_keys=related,
                injected=injected,
            )
        )

    def new_event_id(self) -> str:
        event_id = f"EVT-{self._next_event_number:09d}"
        self._next_event_number += 1
        return event_id

    def suppress_events(self, candidate: _Candidate) -> None:
        for index in candidate.event_indices:
            self.p.delivery_events[index].batch = REMOVED_BATCH

    def order_placed(self, candidate: _Candidate) -> Placed:
        return self.p.orders[candidate.order_index]

    def build_pool(self) -> _OrderPicker:
        order_indices: dict[str, list[int]] = {}
        for index, placed in enumerate(self.p.orders):
            order_indices.setdefault(placed.row[0], []).append(index)
        event_indices: dict[str, list[int]] = {}
        for index, placed in enumerate(self.p.delivery_events):
            event_indices.setdefault(placed.row[1], []).append(index)
        candidates: list[_Candidate] = []
        for order_id in sorted(order_indices):
            indices = order_indices[order_id]
            placed = self.p.orders[indices[0]]
            if len(indices) != 1 or placed.row[O_STATUS] != "delivered":
                continue
            events = tuple(event_indices.get(order_id, []))
            pings = tuple(i for i in events if self.p.delivery_events[i].row[E_TYPE] == "gps_ping")
            candidates.append(_Candidate(order_id, placed.batch, indices[0], events, pings))
        return _OrderPicker(candidates, self.rng)

    def has_next_batch(self, candidate: _Candidate) -> bool:
        return candidate.batch < self.config.n_batches

    # ------------------------------------------------------------ natural cases ---

    def natural_updates(self) -> None:
        """Later versions of orders that were in progress at a cutoff (not injected)."""
        by_order: dict[str, list[Placed]] = {}
        for placed in self.p.orders:
            by_order.setdefault(placed.row[0], []).append(placed)
        for order_id in sorted(by_order):
            versions = by_order[order_id]
            if len(versions) < 2:
                continue
            scenario = self.scenario("newer_order_update")
            for placed in versions[1:]:
                self.add(
                    scenario,
                    "newer_order_update",
                    SOURCE_ORDERS,
                    placed.batch,
                    order_id,
                    ACCEPTED,
                    injected=False,
                )

    # ----------------------------------------------------------- driver cases ---

    def driver_cases(self, plan: DriverPlan) -> None:
        for number, driver_index in enumerate(plan.idle_driver_indices):
            column, value, reason = INVALID_DRIVER_VALUES[number % len(INVALID_DRIVER_VALUES)]
            placed = self.p.drivers[driver_index]
            placed.row = _with(placed.row, column, value)
            scenario = self.scenario("invalid_driver_value")
            self.add(
                scenario,
                "invalid_driver_value",
                SOURCE_DRIVERS,
                placed.batch,
                placed.row[0],
                REJECTED,
                reason,
            )
        for driver_index, window in plan.delayed_driver_windows:
            placed = self.p.drivers[driver_index]
            driver_id = placed.row[0]
            placed.batch = window + 1
            scenario = self.scenario("delayed_driver_record")
            self.add(
                scenario,
                "delayed_driver_record",
                SOURCE_DRIVERS,
                placed.batch,
                driver_id,
                ACCEPTED,
            )
            for shift_placed in self.p.shift_events:
                if shift_placed.row[S_DRIVER] == driver_id and shift_placed.batch == window:
                    self.add(
                        scenario,
                        "orphan_shift_event_missing_driver",
                        SOURCE_SHIFTS,
                        window,
                        shift_placed.row[0],
                        PENDING,
                        "missing_driver",
                        (driver_id,),
                    )

    # ------------------------------------------------------------ order cases ---

    def order_cases(self, picker: _OrderPicker) -> None:
        self._exact_duplicates(picker)
        self._unchanged_resends(picker)
        self._stale_versions(picker)
        self._rejected_orders(picker)
        self._status_normalisation(picker)
        self._delayed_orders(picker)

    def _exact_duplicates(self, picker: _OrderPicker) -> None:
        defect = "exact_duplicate_row"
        for _ in range(self.config.defect_count(defect)):
            candidate = picker.take(defect, lambda c: True)
            placed = self.order_placed(candidate)
            self.p.orders.append(Placed(placed.batch, placed.row))
            scenario = self.scenario(defect)
            self.add(scenario, defect, SOURCE_ORDERS, placed.batch, candidate.order_id, DUPLICATE)

    def _unchanged_resends(self, picker: _OrderPicker) -> None:
        defect = "unchanged_order_resend"
        for _ in range(self.config.defect_count(defect)):
            candidate = picker.take(defect, self.has_next_batch)
            placed = self.order_placed(candidate)
            self.p.orders.append(Placed(placed.batch + 1, placed.row))
            scenario = self.scenario(defect)
            self.add(
                scenario, defect, SOURCE_ORDERS, placed.batch + 1, candidate.order_id, DUPLICATE
            )

    def _stale_versions(self, picker: _OrderPicker) -> None:
        defect = "stale_order_version"
        for _ in range(self.config.defect_count(defect)):
            candidate = picker.take(defect, self.has_next_batch)
            placed = self.order_placed(candidate)
            order = self._orders_by_id[candidate.order_id]
            if order.assigned is None:
                raise GenerationError(f"Delivered order {order.order_id} has no assignment time.")
            older = order_row(order, order.assigned, "assigned", self.world.drivers)
            self.p.orders.append(Placed(placed.batch + 1, older))
            scenario = self.scenario(defect)
            self.add(
                scenario,
                defect,
                SOURCE_ORDERS,
                placed.batch + 1,
                candidate.order_id,
                STALE_IGNORED,
                "older_source_updated_at",
            )

    def _reject_order(
        self, picker: _OrderPicker, defect: str, reason: str, corrupt: Callable[[Row, int], Row]
    ) -> None:
        for number in range(self.config.defect_count(defect)):
            candidate = picker.take(defect, lambda c: True)
            placed = self.order_placed(candidate)
            placed.row = corrupt(placed.row, number)
            self.suppress_events(candidate)
            scenario = self.scenario(defect)
            self.add(
                scenario,
                defect,
                SOURCE_ORDERS,
                placed.batch,
                candidate.order_id,
                REJECTED,
                reason,
            )

    def _rejected_orders(self, picker: _OrderPicker) -> None:
        def blank_required(row: Row, number: int) -> Row:
            return _with(row, MISSING_VALUE_FIELDS[number % len(MISSING_VALUE_FIELDS)], "")

        def bad_status(row: Row, number: int) -> Row:
            return _with(row, O_STATUS, INVALID_STATUSES[number % len(INVALID_STATUSES)])

        def delivery_first(row: Row, number: int) -> Row:
            pickup = parse_utc(row[O_PICKUP])
            if pickup is None:
                raise GenerationError(f"Delivered order {row[0]} has no pickup_ts.")
            earlier = pickup - 300 - int(self.rng.integers(0, 1501))
            return _with(row, O_DELIVERY, format_utc(earlier))

        def unparseable(row: Row, number: int) -> Row:
            field = UNPARSEABLE_FIELDS[number % len(UNPARSEABLE_FIELDS)]
            value = UNPARSEABLE_VALUES[(number // len(UNPARSEABLE_FIELDS)) % 3]
            return _with(row, field, value)

        def out_of_range(row: Row, number: int) -> Row:
            variant = number % 4
            if variant == 0:
                return _with(row, O_FARE, "0.00")
            if variant == 1:
                return _with(row, O_DISTANCE, "0.000")
            if variant == 2:
                return _with(row, O_FARE, "-" + row[O_FARE])
            return _with(row, O_DISTANCE, "-" + row[O_DISTANCE])

        self._reject_order(
            picker, "missing_required_value", "missing_required_field", blank_required
        )
        self._reject_order(picker, "invalid_status", "invalid_status", bad_status)
        self._reject_order(
            picker, "delivery_before_pickup", "delivery_before_pickup", delivery_first
        )
        self._reject_order(picker, "unparseable_timestamp", "invalid_timestamp", unparseable)
        self._reject_order(picker, "invalid_numeric_range", "out_of_range_value", out_of_range)

    def _status_normalisation(self, picker: _OrderPicker) -> None:
        defect = "status_needs_normalisation"
        for number in range(self.config.defect_count(defect)):
            candidate = picker.take(defect, lambda c: True)
            placed = self.order_placed(candidate)
            variant = UNNORMALISED_DELIVERED[number % len(UNNORMALISED_DELIVERED)]
            placed.row = _with(placed.row, O_STATUS, variant)
            scenario = self.scenario(defect)
            self.add(
                scenario,
                defect,
                SOURCE_ORDERS,
                placed.batch,
                candidate.order_id,
                ACCEPTED,
                "status_normalised",
            )

    def _delayed_orders(self, picker: _OrderPicker) -> None:
        defect = "delayed_order_record"
        for _ in range(self.config.defect_count(defect)):
            candidate = picker.take(
                defect, lambda c: self.has_next_batch(c) and len(c.event_indices) > 0
            )
            placed = self.order_placed(candidate)
            original_batch = placed.batch
            placed.batch = original_batch + 1
            scenario = self.scenario(defect)
            self.add(scenario, defect, SOURCE_ORDERS, placed.batch, candidate.order_id, ACCEPTED)
            for index in candidate.event_indices:
                event = self.p.delivery_events[index]
                self.add(
                    scenario,
                    "orphan_event_missing_order",
                    SOURCE_EVENTS,
                    event.batch,
                    event.row[E_ID],
                    PENDING,
                    "missing_order",
                    (candidate.order_id,),
                )

    # ------------------------------------------------------------ event cases ---

    def event_cases(self, picker: _OrderPicker) -> None:
        self._replays(picker)
        self._coordinate_defects(picker)
        self._impossible_speeds(picker)
        self._duplicate_timestamps(picker)
        self._out_of_order(picker)
        self._long_gaps(picker)
        self._late_events(picker)

    def _random_index(self, indices: tuple[int, ...], low: int = 0, high_offset: int = 0) -> int:
        """Random position in ``indices[low : len - high_offset]``."""
        return int(self.rng.integers(low, len(indices) - high_offset))

    def _replays(self, picker: _OrderPicker) -> None:
        for defect, batch_shift in (
            ("replayed_event_same_batch", 0),
            ("replayed_event_next_batch", 1),
        ):
            for _ in range(self.config.defect_count(defect)):
                eligible: Callable[[_Candidate], bool] = (
                    self.has_next_batch if batch_shift else (lambda c: True)
                )
                candidate = picker.take(defect, eligible)
                event = self.p.delivery_events[
                    candidate.event_indices[self._random_index(candidate.event_indices)]
                ]
                self.p.delivery_events.append(Placed(event.batch + batch_shift, event.row))
                scenario = self.scenario(defect)
                self.add(
                    scenario,
                    defect,
                    SOURCE_EVENTS,
                    event.batch + batch_shift,
                    event.row[E_ID],
                    DUPLICATE,
                )

    def _coordinate_defects(self, picker: _OrderPicker) -> None:
        defect = "missing_coordinates"
        for _ in range(self.config.defect_count(defect)):
            candidate = picker.take(defect, lambda c: len(c.ping_indices) >= 1)
            ping = self.p.delivery_events[
                candidate.ping_indices[self._random_index(candidate.ping_indices)]
            ]
            ping.row = _with(_with(ping.row, E_LAT, ""), E_LON, "")
            scenario = self.scenario(defect)
            self.add(
                scenario,
                defect,
                SOURCE_EVENTS,
                ping.batch,
                ping.row[E_ID],
                REJECTED,
                "missing_coordinates",
            )
        defect = "out_of_range_coordinates"
        for number in range(self.config.defect_count(defect)):
            candidate = picker.take(defect, lambda c: len(c.ping_indices) >= 1)
            ping = self.p.delivery_events[
                candidate.ping_indices[self._random_index(candidate.ping_indices)]
            ]
            excess = 0.5 + 4.5 * float(self.rng.random())
            variant = number % 4
            if variant == 0:
                ping.row = _with(ping.row, E_LAT, format_coordinate(90.0 + excess))
            elif variant == 1:
                ping.row = _with(ping.row, E_LAT, format_coordinate(-90.0 - excess))
            elif variant == 2:
                ping.row = _with(ping.row, E_LON, format_coordinate(180.0 + excess))
            else:
                ping.row = _with(ping.row, E_LON, format_coordinate(-180.0 - excess))
            scenario = self.scenario(defect)
            self.add(
                scenario,
                defect,
                SOURCE_EVENTS,
                ping.batch,
                ping.row[E_ID],
                REJECTED,
                "invalid_coordinates",
            )

    def _impossible_speeds(self, picker: _OrderPicker) -> None:
        defect = "impossible_gps_speed"
        for _ in range(self.config.defect_count(defect)):
            candidate = picker.take(defect, lambda c: len(c.ping_indices) >= 2)
            position = self._random_index(candidate.ping_indices, low=1)
            ping = self.p.delivery_events[candidate.ping_indices[position]]
            previous = self.p.delivery_events[candidate.ping_indices[position - 1]]
            low, high = IMPOSSIBLE_JUMP_KM
            jump_km = low + (high - low) * float(self.rng.random())
            bearing = 2 * math.pi * float(self.rng.random())
            origin = (float(ping.row[E_LAT]), float(ping.row[E_LON]))
            lat, lon = offset_point(origin, jump_km, bearing)
            moved = _with(ping.row, E_LAT, format_coordinate(lat))
            ping.row = _with(moved, E_LON, format_coordinate(lon))
            scenario = self.scenario(defect)
            self.add(
                scenario,
                defect,
                SOURCE_EVENTS,
                ping.batch,
                ping.row[E_ID],
                REJECTED,
                "gps_impossible_speed",
                (previous.row[E_ID],),
            )

    def _duplicate_timestamps(self, picker: _OrderPicker) -> None:
        defect = "duplicate_timestamp"
        for _ in range(self.config.defect_count(defect)):
            candidate = picker.take(defect, lambda c: len(c.ping_indices) >= 1)
            original = self.p.delivery_events[
                candidate.ping_indices[self._random_index(candidate.ping_indices)]
            ]
            bearing = 2 * math.pi * float(self.rng.random())
            lat, lon = offset_point(
                (float(original.row[E_LAT]), float(original.row[E_LON])),
                DUPLICATE_TIMESTAMP_OFFSET_KM,
                bearing,
            )
            new_id = self.new_event_id()
            row = _with(
                _with(_with(original.row, E_ID, new_id), E_LAT, format_coordinate(lat)),
                E_LON,
                format_coordinate(lon),
            )
            self.p.delivery_events.append(Placed(original.batch, row))
            scenario = self.scenario(defect)
            self.add(
                scenario,
                defect,
                SOURCE_EVENTS,
                original.batch,
                new_id,
                REJECTED,
                "gps_non_positive_interval",
                (original.row[E_ID],),
            )

    def _out_of_order(self, picker: _OrderPicker) -> None:
        defect = "out_of_order_event"
        for _ in range(self.config.defect_count(defect)):
            candidate = picker.take(defect, lambda c: len(c.ping_indices) >= 2)
            position = self._random_index(candidate.ping_indices, high_offset=1)
            earlier = self.p.delivery_events[candidate.ping_indices[position]]
            later = self.p.delivery_events[candidate.ping_indices[position + 1]]
            earlier_id = earlier.row[E_ID]
            later_id = later.row[E_ID]
            # After the swap the later ping carries the smaller id, so it comes first in the file.
            earlier.row = _with(earlier.row, E_ID, later_id)
            later.row = _with(later.row, E_ID, earlier_id)
            scenario = self.scenario(defect)
            self.add(
                scenario,
                defect,
                SOURCE_EVENTS,
                later.batch,
                earlier_id,
                ACCEPTED,
                "reordered_by_event_ts",
                (later_id,),
            )

    def _long_gaps(self, picker: _OrderPicker) -> None:
        defect = "gps_long_gap"
        needed = LONG_GAP_REMOVED_PINGS + 2
        for _ in range(self.config.defect_count(defect)):
            candidate = picker.take(defect, lambda c: len(c.ping_indices) >= needed)
            start = self._random_index(
                candidate.ping_indices, low=1, high_offset=LONG_GAP_REMOVED_PINGS
            )
            before = self.p.delivery_events[candidate.ping_indices[start - 1]]
            after = self.p.delivery_events[candidate.ping_indices[start + LONG_GAP_REMOVED_PINGS]]
            for offset in range(LONG_GAP_REMOVED_PINGS):
                self.p.delivery_events[candidate.ping_indices[start + offset]].batch = REMOVED_BATCH
            scenario = self.scenario(defect)
            self.add(
                scenario,
                defect,
                SOURCE_EVENTS,
                after.batch,
                after.row[E_ID],
                ACCEPTED,
                "long_gap_not_a_jump",
                (before.row[E_ID],),
            )

    def _late_events(self, picker: _OrderPicker) -> None:
        defect = "late_event"
        for _ in range(self.config.defect_count(defect)):
            candidate = picker.take(defect, self.has_next_batch)
            event = self.p.delivery_events[
                candidate.event_indices[self._random_index(candidate.event_indices)]
            ]
            event.batch += 1
            scenario = self.scenario(defect)
            self.add(scenario, defect, SOURCE_EVENTS, event.batch, event.row[E_ID], ACCEPTED)

    # ------------------------------------------------------------ shift cases ---

    def shift_replays(self, plan: DriverPlan) -> None:
        defect = "replayed_shift_event"
        excluded = {self.p.drivers[index].row[0] for index, _ in plan.delayed_driver_windows}
        pool = [
            index
            for index, placed in enumerate(self.p.shift_events)
            if placed.row[S_DRIVER] not in excluded
        ]
        used_drivers: set[str] = set()
        remaining = self.config.defect_count(defect)
        for draw in self.rng.permutation(len(pool)):
            if remaining == 0:
                break
            placed = self.p.shift_events[pool[int(draw)]]
            if placed.row[S_DRIVER] in used_drivers:
                continue
            used_drivers.add(placed.row[S_DRIVER])
            self.p.shift_events.append(Placed(placed.batch, placed.row))
            scenario = self.scenario(defect)
            self.add(scenario, defect, SOURCE_SHIFTS, placed.batch, placed.row[0], DUPLICATE)
            remaining -= 1
        if remaining:
            raise GenerationError("Not enough shift events for replayed_shift_event.")


def inject_defects(
    config: GeneratorConfig,
    rng: np.random.Generator,
    world: World,
    placements: Placements,
    plan: DriverPlan,
) -> list[Case]:
    """Inject every defect type into ``placements`` (in place) and return all manifest cases."""
    injector = _Injector(config, rng, world, placements)
    injector.natural_updates()
    injector.driver_cases(plan)
    picker = injector.build_pool()
    injector.order_cases(picker)
    injector.event_cases(picker)
    injector.shift_replays(plan)
    return sorted(injector.cases, key=Case.sort_key)
