from node_dao import NodeDAO
from common import md5, exec_local_cmd
from config import root

from config import DEF_LIMIT

import paramiko
import os
import threading
import hashlib
import shutil

class Node(object):
    def __init__(self, ip='127.0.0.1', rootpath=root):
        self.ip = ip
        self.root = rootpath
        self.incoming = self.root + '/incoming'
        if not (os.path.exists(self.incoming)):
            os.mkdir(self.incoming)
        self.store = self.root + '/store'
        if not (os.path.exists(self.store)):
            os.mkdir(self.store)
        self.dao = NodeDAO()

    def scan(self, incoming=None):
        def subs(path):
            resl = []
            for subpath in os.listdir(path):
                if ('.' != subpath[0]):
                    fullpath = os.sep.join([path, subpath])
                    if not(os.path.islink(fullpath)):
                        if os.path.isfile(fullpath):
                            resl.append(fullpath)
                        elif os.path.isdir(fullpath):
                            resl.extend(subs(fullpath))
            return resl
        if (incoming is None):
            incoming = self.incoming
        return subs(incoming)

    def load_file_info_from_incoming_to_db(self):
        resl = []
        for fullname in self.scan():
            f = open(fullname, 'rb')
            md5 = hashlib.md5(f.read()).hexdigest()
            f.close()
            rel_name = fullname[len(self.incoming):]
            if not self.dao.add_record_to_main(rel_name, md5, os.path.getsize(fullname)):
                resl.append(rel_name)
        return resl

    def load_local_file_to_store(self, limit=DEF_LIMIT):
        for pair in self.dao.fetch_new_from_db(limit):
            fullname, src_md5, size = pair
            src_fullname = self.incoming + fullname
            dest_fullname = self.store + fullname
            dest_path, filename = os.path.split(dest_fullname)
            if not os.path.exists(dest_path):
                os.mkdir(dest_path)
            shutil.copyfile(src_fullname, dest_fullname)
            if (md5(dest_fullname) == src_md5):
                self.dao.update_file_info_to_main({'status': '+1'}, ["fullname='"+fullname+"'"])
                self.dao.add_file_info_to_local(fullname, src_md5, size)

    def del_copied_from_incoming(self):
        for item in self.dao.fetch_finished_from_main():
            del_filename = self.incoming + item[0]
            os.remove(del_filename)



if ('__main__' == __name__):
#    node = Node(rootpath='/home/guochen/sync')
    node = Node()
#    ls = node.scan()
#    for l in ls:
#        print (l)
#    print(len(ls))

    node.load_file_info_from_incoming_to_db()
    node.load_local_file_to_store()

#    l2 = node.scan1()
#    print(len(l2))
#    for l in l1:
#        if not (l in l2):
#            print(l)



#    def scan1(self, basepath=None):
#        if basepath is None:
#            basepath = self.basepath
#        resl = []
#        for path in os.listdir(basepath):
#            resl.append(os.sep.join([basepath, path]))
#        return self.threading_scan_path(resl)
#    def threading_scan_path(self, paths):
#        threads = []
#        resl = []
#        def runner(path):
#            if os.path.isfile(path):
#                resl.append(path)
#            elif os.path.isdir(path):
#                resl.extend(self.scan1(path))
#        for path in paths:
#            _t = threading.Thread(
#                target=runner,
#                args=(path,)
#            )
#            _t.start()
#            threads.append(_t)
#        for _t in threads:
#            _t.join()
#        return resl
