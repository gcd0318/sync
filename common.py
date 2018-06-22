import os
import hashlib
import logging
import paramiko
import subprocess
import time
import threading
from threading import Thread

from config import TIMEOUT_s, SHORT_s, RETRY, SCRIPT_EXECUTE_TIMEOUT_s

# status
INVALID = -2
INIT = -1
NEW = 0
# type
FILE = 0

def cond_to_list(conds):
# cond: [col, comp, val]
    cond_l = []
    if (0 < len(conds)):
        if(list != type(conds[0])):
            conds = [conds]
        for cond in conds:
            val = cond[2]
            if(str == type(val)):
                if(not(val.startswith("'"))):
                    val = "'" + val
                if(not(val.endswith("'"))):
                    val = val + "'"
            else:
                val = str(val)
            cond[2] = val
            cond_l.append(' '.join(cond))
    return cond_l

def md5(filename):
    res = None
    if os.path.isfile(filename):
        hash = hashlib.md5()
        with open(filename,'rb') as f:
            b = f.read(8192)
            while b:
                hash.update(b)
                b = f.read(8192)
        res = hash.hexdigest()
    return res



def remote_exec(cmd, ip, username, password, port=22, no_err=True, timeout=TIMEOUT_s, short_wait=SHORT_s, retry=RETRY):
    resd = {'res': [], 'err': []}
    rt = 0
    if ('' != cmd):
        while (('err' in resd.keys()) and (rt <= retry)):
            resd.pop('err')
            try:
                ssh = paramiko.SSHClient()
                ssh.load_system_host_keys()
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh.connect(hostname=ip, port=port, username=username, password=password)
                if (cmd.split(';')[-1].replace('&', '').split('-')[0].strip() in ('reboot', 'shutdown', 'poweroff')):
                    ssh.exec_command(cmd, timeout=timeout)
                else:
                    stdin, stdout, stderr = ssh.exec_command(cmd, timeout=timeout)
                    resd['res'] = stdout.readlines()
                    resd['err'] = stderr.readlines()
                    if ((0 == len(resd['err'])) or ((not no_err) and (0 < len(resd['res'])))):
                        resd.pop('err')
                    else:
                        rt = rt + 1
            except Exception as err:
                resd['err'] = str(err)
                rt = rt + 1
                time.sleep(short_wait)
            finally:
                ssh.close()
        if (('err' in resd) and (0 < len(resd['err'])) and no_err):
            raise Exception('error on ' + ip + ': ' + str(resd['err']))
        if (not 'err' in resd):
            resd['err'] = ''
    return resd


def exec_cmd(cmd, machine='localhost', username=None, password=None, port=22):
    rtcode = 0
    resl = []
    if ('localhost' == machine):
        rtcode, output = exec_local_cmd(cmd)
    else:
        resd = remote_exec(cmd, machine, username, password, port, no_err, timeout, short_wait, retry)
        if ((not ('err' in resd)) or (0 == len(resd['err']))):
            resl = resd['res']
        else:
            rtcode = -1
    return rtcode, resl

def run_shell_cmd(cmd):
    logging.info("run cmd: %s", cmd)
    try:
        rc = subprocess.call(cmd, shell=True)
        if rc != 0:
            logging.error("Fail to run %s , rc: %s" % (cmd, rc))
    except OSError as e:
        logging.error("Fail to run cmd: %s" % e)
    return rc

def exec_local_cmd(args, shell=True, with_blank=False):
    try:
        process = subprocess.Popen(args, shell=shell, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        rtcode = process.returncode
        output = stdout + stderr
        process.poll()
        if (0 != rtcode):
            output = stderr
    except OSError as e:
        rtcode = e.errno
        output = "Fail to run command: %s" % e
    for l in output.split('\n'):
        if ((0 < len(l)) or with_blank):
            resl.append(l)
    return rtcode, resl


def run_shell_script(args, shell=False, log=logging, timeout=SCRIPT_EXECUTE_TIMEOUT_s, return_output=False):
    command = Command(args)
    return command.run(log=log, shell=shell, timeout=timeout, return_output=return_output)


class Command(object):
    def __init__(self, cmd):
        self.cmd = cmd
        self.process = None
        self.error_message = None

    def run(self, shell=False, log=logging, timeout=SCRIPT_EXECUTE_TIMEOUT_s, return_output=False):
        try:
            def kill_process():
                if self.process.poll() is None:
                    log.error('Kill process')
                    self.process.kill()
                    self.error_message = "shell process killed by timeout, timeout=%s" % timeout
                    log.error(self.error_message)

            self.process = subprocess.Popen(self.cmd, shell=shell, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                            close_fds=True)
            # start timer
            t = threading.Timer(timeout, kill_process)
            t.start()

            # wrap self.process.stdout with a NonBlockingStreamReader object:
            nbsr = NonBlockingStreamReader(self.process.stdout)

            shell_output = ""

            # print the output in real-time
            while self.process.poll() is None:
                line = nbsr.readline(0.1)  # 0.1 secs to let the shell output the result
                if line:
                    shell_output += line
                    log.info(line.rstrip())

            # When the subprocess terminates there might be unconsumed output that still needs to be processed.
            content = nbsr.readline(1)
            if content:
                shell_output += content
                log.info(content)

            # cancel timer
            t.cancel()
            if return_output:
                output = shell_output if self.process.returncode == 0 else self.error_message
                return self.process.returncode, output
            else:
                return self.process.returncode, self.error_message
        except OSError as e:
            error_message = "Fail to run command: %s" % e
            log.error(error_message)
            return e.errno, error_message

class NonBlockingStreamReader:
    def __init__(self, stream):
        '''
        stream: the stream to read from.
                Usually a process' stdout or stderr.
        '''

        self._s = stream
        self._q = Queue()

        def _populateQueue(stream, queue):
            '''
            Collect lines from 'stream' and put them in 'quque'.
            '''

            while True:
                line = stream.readline()
                if line:
                    queue.put(line)
                else:
                    break
                    # raise UnexpectedEndOfStream

        self._t = threading.Thread(target=_populateQueue,
                                   args=(self._s, self._q))
        self._t.daemon = True
        self._t.start()  # start collecting lines from the stream

    def readline(self, timeout=None):
        try:
            return self._q.get(block=timeout is not None,
                               timeout=timeout)
        except Empty:
            return None





def execute(command, *args, **kwargs):
    """
    Execute an command then return its return code.
    :type command: str or unicode
    :param timeout: timeout of this command
    :type timeout: int
    :rtype: (int, str, str)
    """

    timeout = kwargs.pop("timeout", None)
    command_list = [command, ]
    command_list.extend(args)

    ref = {
        "process": None,
        "stdout": None,
        "stderr": None,
    }

    def target():
        ref['process'] = subprocess.Popen(
            " ".join(command_list),
            shell=True,
            close_fds=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        ref['stdout'], ref['stderr'] = ref['process'].communicate()

    thread = Thread(target=target)
    thread.start()
    thread.join(timeout=timeout)
    if thread.is_alive():
        if ref['process'] is not None:
            ref['process'].terminate()
            ref['process'].wait()
        thread.join()

    return ref['process'].returncode, ref['stdout'], ref['stderr']



def remote_cp(local, ip, port, username, password, remote, direction):
    # todo: file or path
    # todo: if path not exist
    # todo: regular expression
    dest = None
    t = paramiko.Transport((ip, port))
    t.connect(username=username, password=password)
    localpath = self.basepath
    if ('/' in local):
        localpath = localpath + local[:local.rfind('/')] + '/'
    try:
        if ('l2r' == direction):
            src = localpath + local.split('/')[-1]
            dest = remote + local.split('/')[-1]
            paramiko.SFTPClient.from_transport(t).put(src, dest)
        elif ('r2l' == direction):
            if (not os.path.exists(localpath)):
                os.makedirs(localpath)
            src = remote
            if (local.endswith('/')):
                dest = localpath + remote.split('/')[-1]
            else:
                dest = local
            paramiko.SFTPClient.from_transport(t).get(src, dest)
    except Exception as err:
        dest = None
    finally:
        t.close()
    return dest



if ('__main__' == __name__):
   print(md5('/home/gcd0318/ArchLinuxARM-armv7-latest.tar.gz'))