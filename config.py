host_ips = ['192.168.29.173', '192.168.31.123']
dbname = 'sync'
username = 'sync'
password = 'sync'
port = 5432

root = '/mnt/sda1/sync'

period = 600
rate = 0.9

SHORT_s = 10
TIMEOUT_s = 10
RETRY = 3
SCRIPT_EXECUTE_TIMEOUT_s = 1800  #30 mins

DEF_COPY_NUM = 2
DEF_LIMIT = 10