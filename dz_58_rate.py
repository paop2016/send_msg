# -*- coding:utf-8 -*-
'扫库程序、发送程序。 两小时执行一次'
import sys,re,json,time
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
tc58_tele=set()
result=[]
def feed_set():
    conn=open_db('spider')
    cursor=conn.cursor()
    for i in range(0,20000000,5000000):
        print i
        cursor.execute('select tele from tc58_jobs_master where id>%s and id<=%s'%(i,i+5000000))
        for item in cursor.fetchall():
            tc58_tele.add(item[0])

# 有效手机号码
c = re.compile(r'^1\d{10}$')
def is_valid(phone_num):
    return c.search(phone_num)
def get_rate(name):
    conn = open_db(name)
    cursor = conn.cursor()
    if name=='boss':
        cursor.execute('select account from login_info INNER JOIN user on login_info.user_id=user.id where identity=1')
    else:
        cursor.execute('select account from login_info INNER JOIN user_boss on user_boss.user_id=login_info.user_id')
    result=cursor.fetchall()
    sum=0
    count=0
    for item in result:
        tele=item[0]
        if is_valid(tele):
            sum+=1
            if tele in tc58_tele:
                count+=1
    rate=float(count)/sum
    print '【%s】计算完成，总共有%s个重叠，重叠率%s'%(name,count,rate)

def start():
    feed_set()
    get_rate('boss')
    get_rate('dz')
# start()
print float(0)/0