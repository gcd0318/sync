from config import dbname, username, password
from common import cond_to_list
from db import DB

from common import INVALID, NEW, READY, COPYING, ARCHIVED

class NodeDAO(object):
    def __init__(self):
        self.db = DB(dbname, username, password)

    def fetch_new_from_db(self, limit=None):
        return self.db.select('main', ['path'], ['status='+str(NEW)]，limit=limit)

    def save_new_to_db(self, filelist):
        tmp_conds = []
        res = self.db.select(tablename, 'status',
                             ["path='" + path + "'", "md5<>''", 'status<='+str(NEW)],
                             limit=1)
        if ((res is None) or (0 == len(res))):



if ('__main__' == __name__):
    dao = NodeDAO()
    print(dao.load_from_basepath(['md5', 'like', '%asd%']))
    print(dao.load_from_basepath([['size', '=', 123]]))