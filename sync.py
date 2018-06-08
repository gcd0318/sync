from db import DB
from config import *


import logging
import logging.handlers
logger = logging.getLogger()
fh = logging.handlers.TimedRotatingFileHandler('wp.log', "D", 1, 10)
fh.setFormatter(logging.Formatter('%(asctime)s %(filename)s_%(lineno)d: [%(levelname)s] %(message)s', '%Y-%m-%d %H:%M:%S'))
logger.addHandler(fh)
logger.setLevel(logging.DEBUG)

dbs = []
for host in hosts:
    dbs.append(DB(dbname, username, password, host))
