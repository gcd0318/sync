from config import dbname, username, password
from common import cond_to_list
from db import DB

from common import INVALID, INIT, NEW
from common import FILE
from config import DEF_COPY_NUM

class NodeDAO(object):
    def __init__(self):
        self.db = DB(dbname, username, password)

    def fetch_new_from_db(self, limit=None):
        return self.db.select('main', ['fullname', 'md5', 'size'], ['status='+str(NEW)], limit)

    def add_record_to_main(self, fullname, md5, size):
        res = self.is_fullname_in_main(fullname)
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


    def is_fullname_in_db(self, tablename, fullname):
        res = self.db.select(tablename, ['fullname'], ["fullname='" + fullname + "'"])
        return bool(res)

    def is_fullname_in_main(self, fullname):
        return self.is_fullname_in_db('main', fullname)

    def is_fullname_in_local(self, fullname):
        return self.is_fullname_in_db('local', fullname)

    def update_file_info_to_main(self, val_dict, conds=[]):
        return self.db.update('main', val_dict, conds)

    def update_file_info_to_local(self, val_dict, conds=[]):
        return self.db.update('local', val_dict, conds)

    def add_file_info_to_local(self, fullname, md5, size):
        res = self.is_fullname_in_local(fullname)
        if not res:
            _, res = self.db.insert('local',
                                    fullname=fullname,
                                    md5=md5,
                                    type=FILE,
                                    size=str(size)
                                    )
        return res

    def fetch_synced_from_main(self):
        return self.db.select('main', ['fullname'], ['copy_num=status'])

    def fetch_need_sync_from_main(self):
        return self.db.select('main', ['fullname'], ['copy_num>status'])

    def del_from_db(self, tablename, conds):
        return



if ('__main__' == __name__):
    dao = NodeDAO()
    print(dao.fetch_new_from_db())
    print(dao.fetch_new_from_db())