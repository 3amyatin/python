from __future__ import annotations  # fällt in Python 3.10 aus
from contextlib import ContextDecorator
import time
from typing import Any, Optional


class TimerError(Exception):
  pass


class Timer(ContextDecorator):
  time: Optional[float] = 0

  def __init__(self, iterations: int = 1, unit: str = 'iteration'):
    self.iterations = iterations
    self.unit = unit

  def __enter__(self) -> Timer:
    self.start()
    return self

  def start(self) -> None:
    if self.time: raise TimerError("Timer is running. Use .stop() to stop it")
    self.time = time.perf_counter()

  def __exit__(self, *exc_info: Any) -> None:
    self.stop()

  def stop(self) -> float:
    if not self.time: raise TimerError("Timer is not running. Use .start() to start it")

    duration = time.perf_counter() - self.time
    self.time = None

    print(f"Duration: {duration:0.3f} sec" +(f", {self.iterations} {self.unit}s, {self.iterations / duration :0.3f} {self.unit}s per sec" if self.iterations > 1 else '' ))
    return duration
  # /def stop

# /class Timer
