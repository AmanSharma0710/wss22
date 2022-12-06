import logging
import pickle
from typing import Any

from base import Worker
from mrds import MyRedis
from constants import COUNT, IN, FNAME, OUT

class WcWorker(Worker):
  def run(self, **kwargs: Any) -> None:
    rds: MyRedis = kwargs['rds']
    word_count = {}
    remaining = rds.rds.xlen(IN) - rds.rds.xpending(IN, Worker.GROUP)['pending']
    while remaining > 0:
      file = rds.get_file(self.name)
      #logging.info(f"Processing {file}")
      with open(file, 'r') as f:
        for line in f:
          for word in line.split():
            word_count[word] = word_count.get(word, 0) + 1
      #logging.info(f"Done processing {file}")
      remaining = rds.rds.xlen(IN) - rds.rds.xpending(IN, Worker.GROUP)['pending']
    with open(f"wc_{self.name}.pkl", 'wb') as f:
      pickle.dump(word_count, f, pickle.HIGHEST_PROTOCOL)
    rds.add_output_file(f"wc_{self.name}.pkl")
    logging.info("Output file generated and exiting")