from node_dao import NodeDAO
from common import md5, exec_local_cmd
from config import root, host_ips

from config import DEF_LIMIT

import paramiko
import os
import threading
import hashlib
import shutil
import socket

class Node(object):
    def __init__(self, ip='127.0.0.1', rootpath=root, hosts=[]):
        self.ip = ip
        self.root = rootpath
        self.incoming = self.root + '/incoming'
        if not (os.path.exists(self.incoming)):
            os.mkdir(self.incoming)
        self.store = self.root + '/store'
        if not (os.path.exists(self.store)):
            os.mkdir(self.store)
        self.dao = NodeDAO()
        self.remote_daos = []
        hostname, _, ips = socket.gethostbyname_ex(socket.gethostname())
        for ip in hosts:
            if not(ip in ips):
                self.remote_daos.append(NodeDAO(ip))

    def scan(self, path=None):
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
        if (path is None):
            path = self.incoming
        return subs(path)

    def get_free_size(self):
        stinfo = os.statvfs(self.root)
        return stinfo.f_bavail * stinfo.f_bsize


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

    def del_synced_from_incoming(self):
        resl = []
        for item in self.dao.fetch_synced_from_main():
            del_filename = self.incoming + item[0]
            os.remove(del_filename)
            if os.path.exists(del_filename):
                if not self.dao.del_from_db('main', ['fullname=' + item[0]]):
                    resl.append(del_filename)
        return resl


    def load_from_remote(self):
        pass

    def get_status(self):
        res = {}
        res['free_size'] = self.get_free_size()
        res['incoming_files'] = self.scan(self.incoming)
        res['store_files'] = self.scan(self.store)
        return res



if ('__main__' == __name__):
    node = Node()
    print(node.get_status())


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
