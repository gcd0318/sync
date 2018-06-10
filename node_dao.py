from config import dbname, username, password
from common import cond_to_list
from db import DB

from common import INVALID, INIT, NEW, READY, COPYING, ARCHIVED
from common import FILE
from config import DEF_COPY_NUM

class NodeDAO(object):
    def __init__(self):
        self.db = DB(dbname, username, password)

    def fetch_new_from_db(self, limit=None):
        return self.db.select('main', ['fullname', 'md5'], ['status='+str(NEW)], limit)

    def init_record_to_db(self, fullname, md5, size):
        res = self.is_fullname_in_db(fullname)
        if not res:
            _, res = self.db.insert('main',
                                    fullname=fullname,
                                    md5=md5,
                                    status=NEW,
                                    copy_num=DEF_COPY_NUM,
                                    type=FILE,
                                    size=str(size)
                                    )
        return res


    def is_fullname_in_db(self, fullname):
        res = self.db.select('main', ['fullname'], ["fullname='" + fullname + "'"])
        return bool(res)

    def update_file_info(self, val_dict, conds=[]):
        return self.db.update('main', val_dict, conds)

if ('__main__' == __name__):
    dao = NodeDAO()
    print(dao.fetch_new_from_db())
    print(dao.fetch_new_from_db())