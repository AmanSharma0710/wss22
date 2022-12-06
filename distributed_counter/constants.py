from typing import Final

LOGFILE: Final[str] = "/tmp/wc.log"
N_WORKERS: Final[int] = 4
GLOB: Final[str] = "../books/*.txt"
IN: Final[bytes] = b"files"
OUT: Final[bytes] = b"files_out"
FNAME: Final[bytes] = b"fname"
COUNT: Final[bytes] = b"count"