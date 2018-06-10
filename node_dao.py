from config import dbname, username, password
from common import cond_to_list
from db import DB

from common import INVALID, NEW, READY, COPYING, ARCHIVED

class NodeDAO(object):
    def __init__(self):
        self.db = DB(dbname, username, password)

    def fetch_new_from_db(self, limit=None):
        return self.db.select('main', ['path'], ['status='+str(NEW)], limit)

    def record_new_to_db(self, fullname):
        res = self.db.select('main', 'status', ["fullname='" + fullname + "'", "md5<>''", 'status<='+str(NEW)], limit=1)
        if res:
            pass



if ('__main__' == __name__):
    dao = NodeDAO()
    print(dao.fetch_new_from_db())
    print(dao.fetch_new_from_db())