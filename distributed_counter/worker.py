import logging
from typing import Any

from base import Worker
from mrds import MyRedis
from constants import COUNT, IN

class WcWorker(Worker):
  def run(self, **kwargs: Any) -> None:
    rds: MyRedis = kwargs['rds']
    word_count = {}
    remaining = rds.rds.xlen(IN) - rds.rds.xpending(IN, Worker.GROUP)['pending']
    while remaining > 0:
      file = rds.get_file(self.name)
      logging.info(f"Processing {file}")
      with open(file, 'r') as f:
        for line in f:
          for word in line.split():
            word_count[word] = word_count.get(word, 0) + 1
      logging.info(f"Done processing {file}")
      remaining = rds.rds.xlen(IN) - rds.rds.xpending(IN, Worker.GROUP)['pending']
    for word, count in word_count.items():
      rds.rds.zincrby(COUNT, count, word)
    logging.info("Exiting")