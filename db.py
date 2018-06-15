import logging
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
#        print('init db')
        logging.info('init')

    def __del__(self):
        self.conn.close()

    def execute(self, sql):
        sql = sql.strip()
        if(not(sql.endswith(';'))):
            sql = sql + ';'
#        print('running sql: ' + sql)
        logging.debug('running sql: ' + sql)
        self.cur.execute(sql)


    def update(self, tablename, val_dict, conds=[]):
        sql = 'update ' + tablename + ' set ' + self.kv_to_sql(val_dict) + ' where ' + self.conds_to_sql(conds) + ';'
        try:
            self.execute(sql)
        except Exception as err:
#            print('sql error: ' + str(err))
            logging.error('sql error: ' + str(err))
        finally:
            self.cur.execute('commit;')
        tmpres = self.select(tablename, list(val_dict.keys()), conds)
        resl = []
        if tmpres:
#            print(res)
            resl = tmpres[-1]
#        print(resl)
        res = True
        vlist = list(val_dict.values())
        for v in resl:
            res = res and (str(v).strip() in vlist)
        return res

        pass
    def delete(self, tablename, conds=[]):
        sql = 'delete from ' + tablename + ' where ' + self.conds_to_sql(conds) + ';'
        try:
            self.execute(sql)
        except Exception as err:
            #                print('sql error: ' + str(err))
            logging.error('sql error: ' + str(err))
        finally:
            self.cur.execute('commit;')

        res = self.select(tablename, '*', conds)
        resl = []
        if res:
            resl.append(res[-1][0])
        return res, 0 == len(resl)

    def get_all_cols(self, table):
        res = []
        try:
            self.execute('desc ' + table + ';')
            for col in self.cur.fetchall():
                res.append(col[0])
        except Exception as err:
#            print('sql error: ' + str(err))
            logging.error('sql error: ' + str(err))
        finally:
            self.cur.execute('commit;')
        return res

    def drop_table(self, table_name):
        res = (self.get_table(table_name) is None)
        if (not res):
            sql = 'drop table ' + table_name + ';'
            try:
                self.execute(sql)
            except Exception as err:
#                print('sql error: ' + str(err))
                logging.error('sql error: ' + str(err))
            finally:
                self.cur.execute('commit;')
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
            try:
                self.execute(sql)
            except Exception as err:
#                print('sql error: ' + str(err))
                logging.error('sql error: ' + str(err))
            finally:
                self.cur.execute('commit;')
        return self.table_exists(table_name)

    def get_table(self, table_name):
        return self.select(pg_tables, 'tablename', ["tablename = '" + table_name, "tableowner = '" + self.username])

    def kv_to_sql(self, cond):
        for k in cond:
            v = cond[k]
            if (int == type(v)):
                v = str(v)
            elif (str == type(v)):
                if(v[0] in '+-'):
                    v = k + v
                else:
                    v = "'" + v + "'"
        return str(k) + '=' + v

    def conds_to_sql(self, conds):
        res = '1=1'
        if(0 < len(conds)):
            if (list != type(conds)):
                conds = [conds]
            for cond in conds:
                if (str == type(cond)):
                    res = res + ' and ' + cond
                elif(dict == type(cond)):
                    res = res + ' and ' + self.kv_to_sql(cond)
        return res

    def select(self, table, cols, conds=[], limit=None):
#        print (table, cols, conds, limit)
        res = None
        sql = 'select '
        if (str == type(cols)):
            cols = [cols]
        sql = sql + ', '.join(cols) + ' from ' + table + ' where ' + self.conds_to_sql(conds)
        if (limit is not None):
            sql = sql + ' limit ' + str(limit)
        self.execute(sql+';')
        try:
            res = self.cur.fetchall()
        except Exception as err:
#            print(err)
            logging.warning('sql error: ' + str(err))
        return res

    def insert(self, table, **vallist):
        resl = []
        res = -1
        conds = []
        cols = list(vallist.keys())
        vals = []
        tmpl = []
        for col in cols:
            v = vallist[col]
            if (int == type(v)):
                v = str(v)
            elif(str == type(v)):
                v = "'" + v + "'"
            vals.append(v)
        sql = 'insert into ' + table + ' (' + ', '.join(cols) + ') values ' + ' (' + ', '.join(vals) + ');'
        try:
            self.execute(sql)
        except Exception as err:
#            print('sql error: ' + str(err))
            logging.error('sql error: ' + str(err))
        finally:
            self.cur.execute('commit;')

        res = self.select(table, cols, vallist)
        if res:
#            print(res)
            resl.append(res[-1][0])
#        print(resl)
        return res, 1 == len(resl)

if ('__main__' == __name__):
    db = DB('sync', 'sync', 'sync')
#    print(db.craate_table('testtable', {'col1': 'int', 'col2': 'varchar'}, False))
    print(db.insert('main', fullname='testpath1', md5='testmd5', status=999, copy_num=0, type=999, size=-1))
    print(db.insert('main', fullname='testpath2', md5='testmd5', status=999, copy_num=0, type=999, size=-1))
    print(db.update('main', {'md5':'123'}, ["fullname='testpath1'"]))
    print(db.delete('main', ["fullname='testpath1'"]))
