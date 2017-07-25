# -*- coding:utf-8 -*-
'扫库程序、发送程序。 两小时执行一次'
import sys,re
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
    return db

pool=None
def open_redis():
    global pool
    if pool:
        r = redis.Redis(connection_pool=pool)
    else:
        pool = redis.ConnectionPool(host='127.0.0.1', port=16379,db=0)
        return open_redis()
    return r
a=21
def loop():
    global a
    r=requests.get("http://192.168.1.167:15589/sms/custom.json?production=1&business=8&phone=%s&content=%s&userId=0"%(13167550503,'王凯同学，很遗憾的通知您，您没有达到公司的预期，将于试用期结束后自动解约，请将剩余工作交接给正式员工王长通。 感谢您为Boss直聘的付出，江湖见！'))
    a+=1
    print r.text
for _ in range(1):
    Thread(target=loop).start()
# 处理总量，发短信后注册量，无效注册量(未发短信就注册了)
num=rgt_count=rgt_invalid_count=0
def loop_check(q,l):
    global num,rgt_coun,rgt_invalid_count
    conn = open_db('spider')
    conn.autocommit(True)
    cursor = conn.cursor()
    while True:
        tele=q.get()
        if tele==-1:
            q.put(-1)
            break
        cursor.execute('select id,is_sended from tc58_tele where tele="%s" limit 1'%tele)
        result=cursor.fetchall()
        if result:
            # 未发送短信就来注册了
            if result[0][1]==0:
                cursor.execute('update tc58_tele set is_registed=2 where id=%s' % result[0][0])
                with l:
                    rgt_invalid_count+=1
            # 因发送短信而注册
            elif result[0][1]==1:
                cursor.execute('update tc58_tele set is_registed=1 where id=%s' % result[0][0])
                with l:
                    rgt_coun+=1
        with l:
            num+=1

# 检查注册情况
def check_rgt():
    conn=open_db('boss')
    cursor=conn.cursor()
    r=open_redis()
    start_id=r.get('boss')
    cursor.execute('select id from login_info ORDER BY id desc limit 1')
    end_id =cursor.fetchall()[0][0]
    cursor.execute('select account from login_info where id>%s and id <=%s'%(start_id,end_id))
    result=cursor.fetchall()
    print '新注册%s位boss,开始更新'%len(result)
    q=Queue();l=Lock()
    for tele in result:
        q.put(tele[0])
    q.put(-1)
    ts=[]
    for _ in range(30):
        t=Thread(target=loop_check,args=(q,l))
        t.start()
        ts.append(t)
    for t in ts:
        t.join()
    r.set('boss',end_id)
    print '检查结束,发短信后注册:%s,发短信前注册:%s'%(rgt_count,rgt_invalid_count)
def send_message():
    conn = open_db('spider')
    cursor = conn.cursor()
    timer=datetime.datetime.now().strftime('%Y%m%d')
    cursor.execute('select tele,type from tc58_tele where pulish_date="%s" and is_sended=0 and is_registed=0'%(timer))
    doc0 = '【Boss直聘】等简历很烦？面试才知道不靠谱？招不到牛人？试试BOSS直聘！点击下载 http://zpurl.cn/7iM1 ，退订回TD'
    doc1 = '【Boss直聘】2000万优质求职者，更多专业人才，与精英在线直聊！点击下载 http://zpurl.cn/7iM1 ，退订回TD'
    docs=[doc0,doc1]
    # r=requests.get("http://192.168.1.167:15589/sms/custom.json?production=1&business=8&phone=%s&content=%s&userId=0"%(15804395183,text2))
    # r=requests.get("http://inner-sms.bosszhipin.com:8390/sms/custom.json?production=1&business=8&phone=%s&content=%s&userId=0"%(15804395183,text2))
    for tele in cursor.fetchall():
        print '【%s】%s'%(tele[0],docs[tele[1]])
def start():
    check_rgt()
    # send_message()