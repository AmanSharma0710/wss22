import glob
import logging
import os
import signal
import sys
import time
import pickle
from threading import current_thread

from constants import LOGFILE, N_WORKERS, GLOB, OUT, FNAME, COUNT
from worker import WcWorker
from mrds import MyRedis

workers: list[WcWorker] = []
def sigterm_handler(signum, frame):
  logging.info('Killing main process!')
  for w in workers:
    w.kill()
  sys.exit()


if __name__ == "__main__":
  start_time = time.time()
  # Clear the log file
  open(LOGFILE, 'w').close()
  logging.basicConfig(# filename=LOGFILE,
                      level=logging.DEBUG,
                      force=True,
                      format='%(asctime)s [%(threadName)s] %(levelname)s: %(message)s')
  thread = current_thread()
  thread.name = "client"
  logging.debug('Done setting up loggers.')

  rds = MyRedis()
  for file in glob.glob(GLOB):
    rds.add_file(file)

  signal.signal(signal.SIGTERM, sigterm_handler)
  for i in range(N_WORKERS):
    workers.append(WcWorker(rds=rds))

  for w in workers:
    w.create_and_run(rds=rds)

  logging.debug('Created all the workers')
  while True:
    try:
      pid_killed, status = os.wait()
      logging.info(f"Worker-{pid_killed} died with status {status}!")
    except:
      break
    
  logging.info('Mapping done! Now reducing...')
  last_id = "0"
  for _ in range(rds.rds.xlen(OUT)):
    entry = rds.rds.xread({OUT: last_id}, 1, 0)[0][1][0]
    last_id = entry[0]
    file = entry[1][FNAME]
    print(f"Processing {file}...")
    with open(file, 'rb') as f:
      word_count = pickle.load(f)
      for word, count in word_count.items():
        rds.rds.zincrby(COUNT, count, word)

  for word, c in rds.top(10):
    logging.info(f"{word.decode()}: {int(c)}")
  print("--- %s seconds ---" % (time.time() - start_time))
