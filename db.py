#import logging
import psycopg2

class DB(object):
    def __init__(self, dbname, username, password, host='127.0.0.1', port=5432):
        self.dbname = dbname
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.conn = psycopg2.connect(database=dbname, user=username, password=password, host=host, port=port)
        self.cur = self.conn.cursor()
        print('init db')
#        logging.info('init')

    def __del__(self):
        self.conn.close()

    def execute(self, sql):
        print('running sql: ' + sql)
#        logging.debug('running sql: ' + sql)
        sql = sql.strip()
        if(not(sql.endswith(';'))):
            sql = sql + ';'
        res = None
        try:
            self.cur.execute(sql)
        except Exception as err:
            print('sql error: ' + str(err))
#            logging.error('sql error: ' + str(err))
        if(sql.split()[0] in ('select', 'desc')):
            try:
                res = self.cur.fetchall()
            except Exception as err:
                print(err)
#                logging.warning(err)
        return res


    def update(self):
        pass
    def delete(self):
        pass
    def get_all_cols(self, table):
        res = []
        cols = self.execute('desc ' + table + ';')
        for col in cols:
            res.append(col[0])
        return res

    def drop_table(self, table_name):
        res = (self.get_table(table_name) is None)
        if (not res):
            sql = 'drop table ' + table_name + ';'
            self.execute(sql)
        return self.get_table(table_name) is None

    def table_exists(self, table_name):
        res = self.get_table(table_name)
        return (res is not None)and(0 < len(res))

    def craate_table(self, table_name, cols, force=False):
        if self.table_exists(table_name):
            if force:
                self.drop_table(table_name)
        else:
            sql = 'create table "' + table_name + '"'
            if cols:
                sql = sql + ' ('
                for col_name, datatype in cols.items():
                    sql = sql + '"' + col_name + '" ' + datatype + ','
                sql = sql[:-1] + ')'
            sql = sql + ';'
            self.execute(sql)
        return self.table_exists(table_name)

    def get_table(self, table_name):
        sql = "select tablename from pg_tables where tablename = '" + table_name + "' and tableowner = '" + self.username + "';"
        return self.execute(sql)

    def select(self, table, *cols, limit=None, **conds):
        sql = 'select '
        for col in cols:
            sql = sql + col + ', '
        sql = sql[:-1] + ' from ' + table + ' where 1=1 '
        if(0 < len(conds)):
            for k in conds:
                sql = sql + ' and ' + .join(conds)
        if (limit is not None):
            sql = sql + ' limit ' + str(limit)
        return self.execute(sql+';')

    def insert(self, table, cols, valslist):
        resl = []
        res = -1
        if(0 == len(cols)):
            cols = self.get_all_cols(table)
        conds = []
        sql = 'insert into ' + table + ' (' + ', '.join(cols) + ') values '
        for vals in valslist:
            sql = sql + ' (' + ', '.join(vals) + '),'
            i = 0
            while (i < len(cols)):
                conds.append(cols[i] + '=' + vals[i])
                i = i + 1
        sql = sql[:-1] + ';'
        self.execute(sql)
        res = self.select(table, cols, conds)[-1][0]
        resl.append(res)
        return res, len(resl) == len(valslist)

if ('__main__' == __name__):
    db = DB('testdb', 'auto', 'auto')
    print(db.craate_table('testtable', {'col1': 'int', 'col2': 'varchar'}, False))
    print (db.insert('main', ['path', 'md5', 'status', 'copy_num', 'type', 'size'], {}))
