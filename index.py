# -*- coding:utf-8 -*-
import json
import redis
import base64
import sys
import os
import MySQLdb

current_file_path = os.path.dirname(os.path.realpath(__file__))
# append current path to search paths, so that we can import some third party libraries.
sys.path.append(current_file_path)


def handler(event, context):
    logger = context.getLogger()
    
    rdsServer = context.getUserData("rds_server")
    rdsPort = context.getUserData("rds_port")
    rdsPwd = context.getUserData("rds_password")
    if rdsServer is None:
        rdsServer = '100.125.1.108'
    if rdsPort is None :
        rdsPort = 1033
    if rdsPwd is None:
        rdsPwd = 'Huawei@12'
    print 'MySQL server: %s:%s' %(rdsServer, rdsPort)
    
    db = MySQLdb.connect(rdsServer, "root", rdsPwd, "mysql", int(rdsPort))
    cursor = db.cursor()
    cursor.execute("DROP TABLE IF EXISTS DISData")
    sql = "CREATE TABLE DISData (sn  CHAR(20) NOT NULL, data  CHAR(255))"    
    cursor.execute(sql)    
    testsn = 1
    sql = "SELECT * FROM DISData WHERE sn='%s'" % (testsn)
    
    try:
        # 执行SQL语句
        cursor.execute(sql)
        # 获取所有记录列表
        results = cursor.fetchall()
        for row in results:
            sn = row[0]
            data = row[1]
            print "test data: sn=%s, data=%s" %  (sn, data)
    except:
       print "Error: unable to fecth data"
    
    streamName = event["StreamName"]
    print 'DIS stream name: %s' %(streamName)    
    records = event["Message"]["records"]
    for r in records:
        sn = r["sequence_number"]
        orginalData = r["data"]
        data = base64.b64decode(orginalData)

        # insert DIS data to Mysql db by vpc        
        sql = "INSERT INTO DISData(sn, data) VALUES ('%s', '%s')" %(sn, data)
        try:
            cursor.execute(sql)
            db.commit()
        except:
            print "Error, insert data error"
            db.rollback()
            
        try:
            # read data from Mysql db by vpc
            sql = "SELECT * FROM DISData WHERE sn='%s'" % (sn)
            # 执行SQL语句
            cursor.execute(sql)
            # 获取所有记录列表
            results = cursor.fetchall()
            for row in results:
                sn = row[0]
                data = row[1]
                print "Read data from mysql: sn=%s, data=%s" %  (sn, data)
        except:
           print "Error: unable to fecth data for new read"
    
    db.close()    
    ret = '*** Received %d message from DIS ***' %(len(records))
    print ret
    return ret


if __name__ == '__main__':
    print (__name__)
    handler(0, 0)
