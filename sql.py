from pymysql.err import DatabaseError

import pymysql
import pymysql.cursors


class MySQL:
    def __init__(self, DBHOST, DBPORT, DBUSER, DBPASSWD, DBNAME):
        self.conn = None
        try:
            self.conn = pymysql.connect(host=DBHOST, port=DBPORT, user=DBUSER, passwd=DBPASSWD, db=DBNAME,
                                        cursorclass=pymysql.cursors.DictCursor)
            self.conn.autocommit(1)
        except pymysql.DatabaseError as exc:
            print("Exception found in constructor")
            print(exc)
            exit()

    def executeSQL(self, sql):
        self.conn.connect()
        temp = []
        count = 0
        result = {}
        try:
            cur = self.conn.cursor()
            cur.execute(sql)
            for row in cur:
                temp.append(row)
                count += 1
            result['rows'] = temp
            result['count'] = count
            cur.close()

        except cur.DatabaseError as sql_exception:
            print("Exception found in executeSQL")
            print(sql_exception)
            result['rows'] = temp
            result['count'] = count
            print("returning false")
            return False



        return result

    def escapeString(self, arg):
        return self.conn.escape_string(str(arg))

    def lastInsertId(self):
        return self.conn.insert_id()

    def afftectedRows(self):
        return self.conn.affected_rows()

    def destroy(self):
        return self.conn.close()

    def is_connected(self):
        self.conn.ping(reconnect=True)
        return True


