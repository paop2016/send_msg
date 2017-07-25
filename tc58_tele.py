# -*- coding:utf-8 -*-
'从tc58_jobs_master中，筛选出不包含关键词的数据。每次筛选从上一次筛选的结束id开始，每半小时执行一次。 筛选后的结果存入tc58_tele'
import sys,MySQLdb,platform,redis,re,datetime,time
reload(sys)
sys.setdefaultencoding( "utf-8" )
from threading import Lock, Thread
from Queue import Queue

# 三种拦截器
word_filter=['出国打工','移民','希腊打工','希腊工作','发达国家招','影视剧拍摄','YY兼职','打字','录入','挂机','武汉赴澳洲','哪个都可以','一单只需','美国司机','保安希腊','赴澳服务员',
           '北京悦菲时代','重庆宏亚','濮阳市博栋','酒泉言鼎','信阳市澳海','青岛捷凯' ,'河北金源','厦门澳诚','腾讯','中劳网','闪聘','赶集','个人','先生','女士','小姐','麻将','网络推广','足疗','个体','足浴',
           "人才", "派遣", "劳务", "出入境", "打字", "网络销售","人寿", "太平洋", "保险", "未知", "歌城", "中国平安","出国", "诊所", "演员", "刷单","地产", "代理", "链家", "人力", "酒会", "职介","ktv","KTV", "酒吧", "娱乐", "会所","淘宝","文化", "百姓网"]
phone_rgt_filter=set()
phone_black_filter=set()
# 入库量
num_count=0
result=[]
mac=platform.node()=='MacBook'

def open_db(db_name):
    if db_name=='spider':
        if mac:
            db = MySQLdb.connect(db='spider', port=3308, host='127.0.0.1', user="spider", passwd="b@4RkJFo!6yL", charset="utf8")
        else:
            db = MySQLdb.connect(db='spider', port=3306,host='10.51.178.150', user="spider",passwd="b@4RkJFo!6yL", charset="utf8")
    elif db_name=='boss':
        if mac:
            db = MySQLdb.connect(db='boss', port=3306,host='192.168.1.31', user="boss",passwd="boss", charset="utf8")
        else:
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

def loop(q,l):
    global num_count,result
    # 最后一起提交
    conn = open_db('spider')
    cursor=conn.cursor()
    while True:
        start_index=q.get()
        if start_index==-1:
            q.put(-1)
            break
        end_index=start_index+1000
        items=result[start_index:end_index]
        if items:
            q.put(end_index)
        else:
            q.put(-1)
            break
        # company, publish_date, update_date,tele
        for item in items:
            publish_date=int(item[1].strftime('%Y%m%d'))
            update_date=int(item[2].strftime('%Y%m%d'))
            cursor.execute('select id,publish_date,update_date from tc58_tele where tele="%s" limit 1'%(item[3]))
            result_58=cursor.fetchall()
            # 表里已经存在该电话，更新时间。
            if result_58:
                # 这种情况一般只在首次导入数据时存在
                if publish_date<result_58[0][1]:
                    cursor.execute('update tc58_tele set publish_date="%s" where id=%s'%(publish_date,result_58[0][0]))
                # 这个电话号最近新发了新职位
                if update_date>result_58[0][2]:
                    cursor.execute('update tc58_tele set update_date="%s" where id=%s'%(update_date,result_58[0][0]))
            # 表里没有该电话，插入
            else:
                # 后两位偶数发文案0
                if int(item[3][9:])%2==0:
                    doc_type=0
                # 奇数发文案1
                else:
                    doc_type = 1
                company_name = item[0].replace("'", "''").replace('\\', '\\\\')
                cursor.execute('insert into tc58_tele VALUES (DEFAULT ,"%s","%s","%s","%s","%s","%s","%s",DEFAULT )'%(company_name,item[3],publish_date,publish_date,doc_type,0,0))
            with l:
                num_count+=1
                if num_count%10000==0:
                    print num_count
    conn.commit()
# 电话黑名单多线程入库
def black_loop(q):
    conn_auto = open_db('spider')
    # 自动提交，用于实时存储拦截号码
    conn_auto.autocommit(True)
    cursor_auto = conn_auto.cursor()
    while True:
        items=q.get()
        if items==-1:
            q.put(-1)
            break
        try:
            # 将电话及其公司名存入黑名单
            cursor_auto.execute('insert into tele_black values(Default,"%s","%s")' % (items[0], items[1]))
        except:
            # 唯一键（电话号码）重复，忽略
            pass

# 58同城数据量近两千万，首次入库需要分批查询。
def start_task(start_index,end_index,r):
    global result
    # 处理量、因名字不符被拒绝、已注册拦截量、黑名单拦截量
    num=word_count=rgt_count=black_count=0
    q=Queue()
    l=Lock()
    q.put(0)
    conn = open_db('spider')
    cursor=conn.cursor()
    print '开始查询【%s~%s】'%(start_index,end_index)
    # 查询
    cursor.execute('select title,company,publish_date,tele from tc58_jobs_master where id>%s and id<=%s'%(start_index,end_index))
    print '查询完毕【%s~%s】，开始合并' % (start_index, end_index)
    items=cursor.fetchall()
    # 按【电话号码】合并后的结果
    # key:tele value:[company,publish_date,update_date,tele]
    result_dic={}
    # 电话黑名单queue
    q_black = Queue()
    for _ in xrange(15):
        # 开启向数据库插入电话黑名单的线程
        Thread(target=black_loop,args=(q_black,)).start()
    for item in items:
        num+=1
        if num%10000==0:
            print '合并进度:%s,词汇拦截:%s,注册拦截:%s,黑名单拦截:%s'%(num,word_count,rgt_count,black_count)
        title=item[0]
        company=item[1]
        publish_date=item[2]
        tele=item[3]
        # 任意一项为空则舍弃该条数据
        for i in item:
            if not i:
                break
        else:
            # 电话号码无效，舍弃
            if not is_valid(tele):
                continue
            # 黑名单拦截
            if tele in phone_black_filter:
                black_count += 1
                continue
            # 注册拦截
            if tele in phone_rgt_filter:
                rgt_count += 1
                continue
            # 词汇拦截
            for word in word_filter:
                if word in title or word in company:
                    company = company.replace("'", "''").replace('\\', '\\\\')
                    # 更新电话拦截
                    phone_black_filter.add(tele)
                    q_black.put((company, tele))
                    word_count += 1
                    break
            # 各种规则都符合后，开始整理
            else:
                # 合并
                if tele in result_dic:
                    # publish_date比当前publish_date小，更新
                    if publish_date < result_dic[tele][1]:
                        result_dic[tele][1]=publish_date
                    # publish_date比当前update_date大，更新
                    elif publish_date > result_dic[tele][2]:
                        result_dic[tele][2]=publish_date
                # 新增
                else:
                    result_dic[tele]=[company,publish_date,publish_date,tele]
    q_black.put(-1)
    result=result_dic.values()
    # 释放内存
    del result_dic
    print "【%s~%s】词汇拦截:%s,已注册拦截:%s,黑名单拦截:%s。 得到数据:%s,开始插入"%(start_index, end_index,word_count,rgt_count,black_count,len(result))
    ts=[]
    for _ in xrange(30):
        t=Thread(target=loop,args=(q,l))
        t.start()
        ts.append(t)
    for t in ts:
        t.join()
    # 更新redis中的标记，防止重复读取
    cursor.execute('select id from tc58_jobs_master where id>%s and id<=%s ORDER BY id DESC limit 1'%(start_index,end_index))
    end_id =cursor.fetchall()[0][0]
    r.set('tc58',end_id)
    # 释放内存
    result=[]
    print "【%s~%s】完成" % (start_index, end_index)

def feed_filter():
    print '初始化拦截器'
    conn_spider=open_db('spider')
    conn_boss=open_db('boss')
    cursor_spider=conn_spider.cursor()
    cursor_boss=conn_boss.cursor()

    #已经注册
    cursor_boss.execute('select account from login_info')
    for item in cursor_boss.fetchall():
        if is_valid(item[0]):
            phone_rgt_filter.add(item[0])

    #电话黑名单
    cursor_spider.execute('select tele from tele_black')
    for item in cursor_spider.fetchall():
        phone_rgt_filter.add(item[0])
    print '拦截器初始化成功，词汇：%s个，注册：%s个，黑名单：%s个'%(len(word_filter),len(phone_rgt_filter),len(phone_black_filter))
# 有效手机号码
c = re.compile(r'^1\d{10}$')
def is_valid(phone_num):
    return c.search(phone_num)

# 去除库中已有，但后来被填入黑名单的
def clean():
    print '依照黑名单清除已存数据'
    conn = open_db('spider')
    cursor = conn.cursor()
    cursor.execute('delete from tc58_tele where tele in (select tele from tele_black)')
    conn.commit()

def start():
    # 初始化词汇拦截、已注册拦截、黑名单拦截
    time_start=datetime.datetime.now()
    feed_filter()
    r=open_redis()
    conn = open_db('spider')
    # 从上次执行结束的id开始查询
    start_id=int(r.get('tc58'))
    cursor=conn.cursor()
    cursor.execute('select id from tc58_jobs_master ORDER BY id DESC limit 1')
    end_id=cursor.fetchall()[0][0]
    for i in xrange(start_id,end_id,2500000):
        start_task(i,i+2500000,r)
    clean()
    time_end=datetime.datetime.now()
    timer=time_end-time_start
    print '任务结束，耗时%s秒'%(timer.seconds)
start()
