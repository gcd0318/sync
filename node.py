from node_dao import NodeDAO
from common import md5, exec_local_cmd
from config import root

import paramiko
import os
import threading
import hashlib

class Node(object):
    def __init__(self, ip='127.0.0.1', rootpath=root):
        self.ip = ip
        self.root = rootpath
        self.incoming = self.root + 'incoming'
        self.store = self.root + 'store'
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
            if not self.dao.init_record_to_db(rel_name, md5, os.path.getsize(fullname)):
                resl.append(rel_name)
        return resl



if ('__main__' == __name__):
    node = Node()
#    ls = node.scan()
#    for l in ls:
#        print (l)
#    print(len(ls))

    node.load_file_info_from_incoming_to_db()

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
