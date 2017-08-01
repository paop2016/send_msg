# -*- coding:utf-8 -*-
import sys,re,json,time
from string import upper

reload(sys)
sys.setdefaultencoding( "utf-8" )
import requests,MySQLdb,redis
from threading import Lock, Thread
import datetime
from Queue import Queue

def open_db(db_name):
    if db_name=='spider':
        db = MySQLdb.connect(db='spider', port=3306,host='10.51.178.150', user="spider",passwd="b@4RkJFo!6yL", charset="utf8")
    elif db_name=='boss':
        db = MySQLdb.connect(db='boss', port=3306,host='appdb-05', user="skanzhun",passwd="sIjk!@1*U", charset="utf8")
    elif db_name=='dz':
        db = MySQLdb.connect(db='blue_user', port=3306,host='dzadmin-01', user="search",passwd="Zxt!PKC@Ru5y", charset="utf8")
    return db
def start():
    conn=open_db('spider')
    cursor=conn.cursor()
    cursor.execute('select city from tc58_tele group by city having count(id)>300')
    for item in cursor.fetchall():
        cursor.execute('insert into tc58_city VALUES (DEFAULT ,"%s")'%item[0])
    conn.commit()
start()