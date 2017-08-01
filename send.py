# -*- coding:utf-8 -*-
'扫库程序、发送程序。 两小时执行一次'
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

pool=None
def open_redis():
    global pool
    if pool:
        r = redis.Redis(connection_pool=pool)
    else:
        pool = redis.ConnectionPool(host='127.0.0.1', port=16379,db=0)
        return open_redis()
    return r
# 处理总量，发短信后注册量，无效注册量(未发短信就注册了)
num=rgt_count = rgt_invalid_count = 0

def loop_check(q,l,name):
    global num,rgt_count,rgt_invalid_count
    conn = open_db('spider')
    conn.autocommit(True)
    cursor = conn.cursor()
    while True:
        item = q.get()
        if item == -1:
            q.put(-1)
            break
        if name=='boss':
            tele=item
        elif name=='dz':
            user_id,tele=item
        cursor.execute('select id,is_sended from tc58_tele where tele="%s" limit 1'%tele)
        result=cursor.fetchall()
        if result:
            if name=='boss':
                # 未发送短信就来Boss注册了
                if result[0][1]==0:
                    cursor.execute('update tc58_tele set is_registed=4 where id=%s' % result[0][0])
                    with l:
                        rgt_invalid_count+=1
            elif name=='dz':
                # 未发送短信就来店长注册了
                if result[0][1]==0:
                    cursor.execute('update tc58_tele set is_registed=3 where id=%s' % result[0][0])
                    with l:
                        rgt_invalid_count+=1

                # 因发送短信而在店长注册
                elif result[0][1]==1:
                    # 查询是以什么身份注册的，boss身份优先级高
                    if user_id in dz_boss_ids:
                        cursor.execute('update tc58_tele set is_registed=1 where id=%s' % result[0][0])
                    elif user_id in dz_geek_ids:
                        cursor.execute('update tc58_tele set is_registed=2 where id=%s' % result[0][0])
                    with l:
                        rgt_count+=1
        with l:
            num+=1

# 检查已注册情况
def check_rgt(name):
    if name=='boss':
        table='Boss直聘'
    elif name=='dz':
        table='店长直聘'
    print '开始检查%s新注册用户'%table
    global rgt_count,rgt_invalid_count
    rgt_count = rgt_invalid_count = 0
    conn=open_db(name)
    cursor=conn.cursor()
    r=open_redis()
    start_id=r.get(name)
    cursor.execute('select id from login_info ORDER BY id desc limit 1')
    end_id =cursor.fetchall()[0][0]
    if name=='boss':
        cursor.execute('select account from login_info where id>%s and id <=%s'%(start_id,end_id))
    if name=='dz':
        cursor.execute('select user_id,account from login_info where id>%s and id <=%s'%(start_id,end_id))
    result=cursor.fetchall()
    print '【%s】%s新注册%s位用户,开始更新'%(datetime.datetime.now(),table,len(result))
    q=Queue();l=Lock()
    if name == 'boss':
        for tele in result:
            q.put(tele[0])
    if name == 'dz':
        for tele in result:
            q.put((tele[0],tele[1]))
    q.put(-1)
    ts=[]
    for _ in range(50):
        t=Thread(target=loop_check,args=(q,l,name))
        t.start()
        ts.append(t)
    for t in ts:
        t.join()
    r.set(name,end_id)
    print '【%s】%s更新结束,发短信后注册:%s,发短信前注册:%s'%(datetime.datetime.now(),table,rgt_count,rgt_invalid_count)

dz_boss_ids=set()
dz_geek_ids=set()
def init_dz_set():
    print '初始店长信息'
    conn = open_db('dz')
    cursor = conn.cursor()
    cursor.execute('select user_id from user_boss')
    for user_id in cursor.fetchall():
        dz_boss_ids.add(user_id[0])

    cursor.execute('select user_id from user_geek')
    for user_id in cursor.fetchall():
        dz_geek_ids.add(user_id[0])
    print '初始完毕'

def send_queue(q,l):
    conn = open_db('spider')
    conn.autocommit(True)
    cursor = conn.cursor()
    while True:
        item=q.get()
        if item==-1:
            q.put(-1)
            break
        id, title, city, tele=item
        # r=requests.get("http://114.113.154.13/smsJson.aspx?action=send&account=ADC0093&mobile=%s&content=【店长直聘】%s大量人才在找%s工作，下载店长直聘APP免费看简历免费招人！http://t.cn/R9c3ygz 退订回T&password=D2B6269D4FC77629BC4798A41EFE7217"%(tele,city,title))
        # 新接口判断
        # if json.loads(r.text)['message']=='操作成功':
        if True:
            cursor.execute('update tc58_tele set is_sended=1 where id=%s' % id)
        else:
            # print r.text
            cursor.execute('update tc58_tele set is_sended=2 where id=%s' % id)
        print tele


def send_message():
    print '扫描目标用户..'
    conn = open_db('spider')
    cursor = conn.cursor()
    timer=datetime.datetime.fromtimestamp(time.time()-3600*24*5).strftime('%Y%m%d')
    # 发送逻辑
    cursor.execute('select id,title,city,tele from tc58_tele where update_date>="%s" and is_sended=0 and is_registed=0'%(timer))
    result=cursor.fetchall()
    print '需要发送%s条短信，花费%s元。开始发送..'%(len(result),len(result)*0.03)
    q=Queue()
    l=Lock()
    for item in result:
        q.put((item[0],item[1],item[2],item[3]))
    q.put(-1)
    ts=[]
    for _ in range(5):
        t=Thread(target=send_queue,args=(q,l))
        t.start()
        ts.append(t)
    for t in ts:
        t.join()
    print '【%s】发送完毕'%(datetime.datetime.now())
def start():
    # 初始化店长boss、牛人信息
    init_dz_set()
    check_rgt('boss')
    check_rgt('dz')
    # if datetime.datetime.now().hour==16:
    # send_message()
start()