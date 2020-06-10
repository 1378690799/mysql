__version__= "1.0.0"
import pymysql

class sqlConn():
    host = None
    username = None
    port = None
    password = None
    cursor = pymysql.cursors.DictCursor

    def __init__(self, host, port, username, password, cursor=None):
        if cursor == None:
            self.host = host
            self.username = username
            self.port = port
            self.password = password
    # 初始化对象属性

    def execute(self, dbName, cmd, fetchall=False):
        try:
            conn = pymysql.connect(host=self.host,
                                   port=self.port,
                                   user=self.username,
                                   password=self.password,
                                   db=dbName,
                                   charset="utf8",
                                   cursorclass=self.cursor)
            cur = conn.cursor()
            cur.execute(cmd)
            conn.commit()
            if fetchall == False:
                r_value = cur.fetchone()
            elif fetchall == True:
                r_value = cur.fetchall()
            cur.close()
            conn.close()
            return r_value
        except Exception as e:
            return str(e)
    # 数据库执行

    def create_db(self, dbName):
        r_value = self.execute("sys", "CREATE DATABASE {}".format(dbName))
        return r_value
    # 创建数据库

    def delete_db(self, dbName):
        r_value = self.execute("sys", "DROP DATABASE {}".format(dbName))
        return r_value
    # 删除数据库

    def create_table(self, dbName, tableName, tableDict, primary=None, default=False, engine="InnoDB", charset="utf8"):
        cmd = "CREATE TABLE {}(".format(tableName)
        for colName in tableDict:
            cmd += colName + " " + tableDict[colName] + ","
        cmd = cmd[:-1] + ")" if primary == None else cmd[:-1] + ",PRIMARY KEY({})".format(primary) + ")"
        cmd += "ENGINE={} CHARSET={}".format(engine,
                                             charset) if default == False else "ENGINE=InnoDB DEFAULT CHARSET=utf8".format(
            engine, charset)
        r_value = self.execute(dbName, cmd)
        return r_value
    # 创建数据表

    def delete_table(self, dbName, tableName):
        r_value = self.execute(dbName, "DROP TABLE {}".format(tableName))
        return r_value
    # 删除数据表

    def insert(self, dbName, tableName, dataDict):
        cmd = "INSERT INTO {}(".format(tableName)
        for colName in dataDict:
            cmd += colName + ","
        cmd = cmd[:-1] + ") VALUES("
        for colName in dataDict:
            if isinstance(dataDict[colName], int) or isinstance(dataDict[colName], float):
                cmd += str(dataDict[colName]) + ","
            else:
                cmd += "\'" + dataDict[colName] + "\'" + ","
        cmd = cmd[:-1] + ")"
        r_value = self.execute(dbName, cmd)
        return r_value
    # 插入数据

    def select(self, dbName, tableName, colList=None, where=None, fetchall=False, limit=None, offset=None):
        if colList == None:
            cmd = "SELECT * FROM {}".format(tableName)
        else:
            cmd = "SELECT "
            for colName in colList:
                cmd += colName + ","
            cmd = cmd[:-1] + " FROM {}".format(tableName)
        cmd += " WHERE " + where if where != None else ""
        cmd += " LIMIT " + str(limit) if limit != None else ""
        cmd += " OFFSET " + str(offset) if offset != None else ""
        r_value = self.execute(dbName, cmd, fetchall)
        return r_value
    #查询数据

    def update(self,dbName,tableName,setDict,where=None):
        cmd="UPDATE {} SET ".format(tableName)
        for colName in setDict:
            if isinstance(setDict[colName], int) or isinstance(setDict[colName], float):
                cmd += colName+"="+str(setDict[colName]) + ","
            else:
                cmd += colName+"="+"\'" + setDict[colName] + "\'" + ","
        cmd=cmd[:-1] if where==None else cmd[:-1]+" WHERE "+where
        r_value = self.execute(dbName, cmd)
        return r_value
    #更新数据

    def delete(self, dbName, tableName,where):
        r_value = self.execute(dbName, "DELETE FROM {} WHERE {}".format(tableName,where))
        return r_value
    # 删除数据

    def clear(self,dbName, tableName):
        r_value = self.execute(dbName, "DELETE FROM {}".format(tableName))
        return r_value
    # 清空数据
