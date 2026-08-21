import threading
import time

from binance_data_hub.runtime_tuner import (
    AdjustableConnectionGate,
    candidate_is_better,
)


def test_candidate_requires_meaningful_gain_and_no_error_pressure():
    assert candidate_is_better(4.0, 4.3, candidate_errors=0)
    assert not candidate_is_better(4.0, 4.1, candidate_errors=0)
    assert not candidate_is_better(4.0, 5.0, candidate_errors=3)


def test_adjustable_gate_can_raise_limit_while_waiters_are_blocked():
    gate = AdjustableConnectionGate(1)
    gate.acquire()
    entered = threading.Event()

    def waiter():
        gate.acquire()
        entered.set()
        gate.release()

    thread = threading.Thread(target=waiter)
    thread.start()
    time.sleep(0.05)
    assert not entered.is_set()

    gate.set_limit(2)
    assert entered.wait(1.0)
    gate.release()
    thread.join(timeout=1.0)
    assert gate.active == 0


def test_lowering_gate_blocks_new_connections_until_active_falls_below_limit():
    gate = AdjustableConnectionGate(3)
    gate.acquire()
    gate.acquire()
    gate.acquire()
    gate.set_limit(1)

    entered = threading.Event()

    def waiter():
        gate.acquire()
        entered.set()
        gate.release()

    thread = threading.Thread(target=waiter)
    thread.start()
    time.sleep(0.05)
    assert not entered.is_set()

    gate.release()
    gate.release()
    time.sleep(0.05)
    assert not entered.is_set()

    gate.release()
    assert entered.wait(1.0)
    thread.join(timeout=1.0)
    assert gate.active == 0
