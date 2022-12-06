from __future__ import annotations

import logging
import os
import signal
import sys
from abc import abstractmethod, ABC
from threading import current_thread
from typing import Any, Final


class Worker(ABC):
  GROUP: Final = "worker"

  def __init__(self, **kwargs: Any):
    self.name = "worker-?"
    self.pid = -1

  def create_and_run(self, **kwargs: Any) -> None:
    pid = os.fork()
    assert pid >= 0
    if pid == 0:
      self.pid = os.getpid()
      self.name = f"worker-{self.pid}"
      thread = current_thread()
      thread.name = self.name
      logging.info(f"Starting")
      self.run(**kwargs)
      sys.exit()

  @abstractmethod
  def run(self, **kwargs: Any) -> None:
    raise NotImplementedError

  def kill(self) -> None:
    logging.info(f"Killing {self.name}")
    os.kill(self.pid, signal.SIGKILL)
