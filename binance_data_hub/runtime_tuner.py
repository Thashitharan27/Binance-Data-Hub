"""In-run connection-cap optimization for long Binance archive downloads."""
from __future__ import annotations

import time
from collections import deque
from threading import Condition, Event, Lock, Thread


class AdjustableConnectionGate:
    """Semaphore-like gate whose connection limit can change while a run is active."""

    def __init__(self, limit: int):
        self._condition = Condition()
        self._limit = max(1, int(limit))
        self._active = 0

    def acquire(self):
        with self._condition:
            while self._active >= self._limit:
                self._condition.wait()
            self._active += 1
            return True

    def release(self):
        with self._condition:
            if self._active <= 0:
                raise RuntimeError("connection gate released too many times")
            self._active -= 1
            self._condition.notify_all()

    def set_limit(self, limit: int) -> int:
        with self._condition:
            self._limit = max(1, int(limit))
            self._condition.notify_all()
            return self._limit

    @property
    def limit(self) -> int:
        with self._condition:
            return self._limit

    @property
    def active(self) -> int:
        with self._condition:
            return self._active


class ErrorWindow:
    """Small rolling counter for retryable network/CDN errors."""

    def __init__(self):
        self._lock = Lock()
        self._events = deque()

    def add(self):
        now = time.monotonic()
        with self._lock:
            self._events.append(now)
            self._trim_locked(now, 7200.0)

    def count(self, seconds: float) -> int:
        now = time.monotonic()
        with self._lock:
            # Keep the full two-hour history. Different callers ask for five-
            # minute and one-hour windows; a short query must not erase events
            # that are still relevant to the next hourly decision.
            self._trim_locked(now, 7200.0)
            cutoff = now - max(1.0, float(seconds))
            return sum(1 for item in self._events if item >= cutoff)

    def _trim_locked(self, now: float, seconds: float):
        cutoff = now - seconds
        while self._events and self._events[0] < cutoff:
            self._events.popleft()


def candidate_is_better(
    baseline_mbps: float,
    candidate_mbps: float,
    *,
    candidate_errors: int = 0,
    minimum_gain_pct: float = 5.0,
    error_threshold: int = 3,
) -> bool:
    """Return True only when a higher cap gives a meaningful stable speed gain."""
    baseline = max(0.0, float(baseline_mbps))
    candidate = max(0.0, float(candidate_mbps))
    if int(candidate_errors) >= int(error_threshold):
        return False
    if baseline <= 0:
        return candidate > 0
    required = baseline * (1.0 + max(0.0, float(minimum_gain_pct)) / 100.0)
    return candidate >= required


class RuntimeAutoCalibrator:
    """Periodically optimize the live gate using the download's own traffic.

    The calibrator deliberately does not launch a second synthetic benchmark while
    the real collection is active. Once per interval it measures a baseline,
    temporarily raises the connection cap by one step, measures again, and keeps
    the higher cap only when throughput improves materially without retry pressure.
    Repeated retryable network/CDN errors cause a one-step backoff.
    """

    def __init__(
        self,
        gate: AdjustableConnectionGate,
        meter,
        *,
        enabled: bool = False,
        interval_seconds: float = 3600.0,
        probe_seconds: float = 20.0,
        max_connections: int = 32,
        min_connections: int = 8,
        step: int = 8,
        minimum_gain_pct: float = 5.0,
        error_threshold: int = 3,
        callback=None,
        cancelled=None,
    ):
        self.gate = gate
        self.meter = meter
        self.enabled = bool(enabled)
        self.interval_seconds = max(60.0, float(interval_seconds))
        self.probe_seconds = max(8.0, float(probe_seconds))
        self.max_connections = max(1, int(max_connections))
        self.min_connections = max(1, int(min_connections))
        self.step = max(1, int(step))
        self.minimum_gain_pct = max(0.0, float(minimum_gain_pct))
        self.error_threshold = max(1, int(error_threshold))
        self.callback = callback
        self.cancelled = cancelled
        self.errors = ErrorWindow()
        self.events: list[dict] = []
        self._stop = Event()
        self._thread: Thread | None = None

    def start(self):
        if not self.enabled or self._thread is not None:
            return
        self._thread = Thread(target=self._run, name="binance-data-hub-auto-calibrator", daemon=True)
        self._thread.start()
        self._emit(
            f"Hourly auto calibration enabled — starting cap {self.gate.limit}; "
            f"next recheck in {int(self.interval_seconds // 60)} min."
        )

    def stop(self):
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)

    def report_error(self):
        self.errors.add()

    def _cancelled(self) -> bool:
        return bool(self._stop.is_set() or (self.cancelled and self.cancelled()))

    def _wait(self, seconds: float) -> bool:
        deadline = time.monotonic() + max(0.0, float(seconds))
        while not self._cancelled():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return True
            self._stop.wait(min(1.0, remaining))
        return False

    def _measure(self, seconds: float) -> float:
        before = self.meter.snapshot()
        start_bytes = int(before.get("network_bytes", 0))
        started = time.monotonic()
        if not self._wait(seconds):
            return 0.0
        after = self.meter.snapshot()
        elapsed = max(0.001, time.monotonic() - started)
        delta = max(0, int(after.get("network_bytes", 0)) - start_bytes)
        return delta * 8.0 / elapsed / 1_000_000.0

    def _record(self, *, action: str, before: int, after: int, baseline: float = 0.0, candidate: float = 0.0, errors: int = 0):
        item = {
            "timestamp": time.time(),
            "action": action,
            "before": int(before),
            "after": int(after),
            "baseline_mbps": float(baseline),
            "candidate_mbps": float(candidate),
            "errors": int(errors),
        }
        self.events.append(item)
        return item

    def _emit(self, message: str):
        if self.callback:
            try:
                self.callback(message)
            except Exception:
                pass

    def _run(self):
        while self._wait(self.interval_seconds):
            if self._cancelled():
                return

            current = self.gate.limit
            recent_errors = self.errors.count(self.interval_seconds)
            if recent_errors >= self.error_threshold and current > self.min_connections:
                lowered = max(self.min_connections, current - self.step)
                self.gate.set_limit(lowered)
                self._record(action="backoff", before=current, after=lowered, errors=recent_errors)
                self._emit(
                    f"Auto calibration: {recent_errors} retryable network errors detected; "
                    f"connection cap reduced {current} → {lowered}."
                )
                continue

            candidate = min(self.max_connections, current + self.step)
            if candidate <= current:
                self._emit(
                    f"Auto calibration: cap {current} is already the configured maximum; keeping it."
                )
                continue

            self._emit(
                f"Auto calibration: measuring current cap {current} for {int(self.probe_seconds)} sec..."
            )
            baseline = self._measure(self.probe_seconds)
            if self._cancelled():
                return
            if baseline <= 0:
                self._emit("Auto calibration: no sustained transfer activity; keeping current cap.")
                continue

            error_before = self.errors.count(300.0)
            self.gate.set_limit(candidate)
            self._emit(
                f"Auto calibration: trying {candidate} connections for {int(self.probe_seconds)} sec "
                f"(baseline {baseline:.2f} Mbps)..."
            )
            candidate_speed = self._measure(self.probe_seconds)
            error_after = self.errors.count(300.0)
            candidate_errors = max(0, error_after - error_before)

            keep = candidate_is_better(
                baseline,
                candidate_speed,
                candidate_errors=candidate_errors,
                minimum_gain_pct=self.minimum_gain_pct,
                error_threshold=self.error_threshold,
            )
            if keep:
                self._record(
                    action="raise",
                    before=current,
                    after=candidate,
                    baseline=baseline,
                    candidate=candidate_speed,
                    errors=candidate_errors,
                )
                gain = ((candidate_speed / baseline) - 1.0) * 100.0 if baseline else 0.0
                self._emit(
                    f"Auto calibration: keeping {candidate} connections — {candidate_speed:.2f} Mbps "
                    f"vs {baseline:.2f} Mbps ({gain:+.1f}%)."
                )
            else:
                self.gate.set_limit(current)
                self._record(
                    action="revert",
                    before=candidate,
                    after=current,
                    baseline=baseline,
                    candidate=candidate_speed,
                    errors=candidate_errors,
                )
                self._emit(
                    f"Auto calibration: {candidate} did not improve sustained speed enough "
                    f"({candidate_speed:.2f} vs {baseline:.2f} Mbps, {candidate_errors} errors); "
                    f"restored {current}."
                )
