__author__ = 'houxiang'

import  MySQLdb
import logging

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

fh = logging.FileHandler('log.log')
fh.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)

logger.addHandler(fh)

sourceName=('openhub_project','csdn_ask','csdn_blogs','csdn_topics','cnblog_news','cnblog_question','dewen_question',
            'freecode_projects','iteye_ask','oschina_project','oschina_question','sourceforge_project','stackoverflow_q'
            ,'51cto_blog','codeproject','lupaworld','iteye_blog','gna','apache','phpchina','softpedia','linuxtone','slashdot'
            ,'neitui','lagou')

dayFlow={"csdn_ask":0,"csdn_blogs":0,"csdn_topics":0,"cnblog_news":0,"cnblog_question":0,"dewen_question":0,
         "iteye_ask":0,"oschina_question":0,"stackoverflow_q":0}

dayFlowRate = {'openhub_project':0.0,'csdn_ask':0.0,'csdn_blogs':0.0,'csdn_topics':0.0,'cnblog_news':0.0,'cnblog_question':0.0,'dewen_question':0.0,
            'freecode_projects':0.0,'iteye_ask':0.0,'oschina_project':0.0,'oschina_question':0.0,'sourceforge_project':0.0,'stackoverflow_q':0.0
            ,'51cto_blog':0.0,'codeproject':0.0,'lupaworld':0.0,'iteye_blog':0.0,'gna':0.0,'apache':0.0,'phpchina':0.0,'softpedia':0.0,'linuxtone':0.0,'slashdot':0.0
            ,'neitui':0.0,'lagou':0.0}
sourceDB={'host':"192.168.80.104","user":"influx","passwd":"influx1234","port":"3306"}
targerDB={'host':"192.168.80.130","user":"trustie","passwd":"1234","port":"3306"}


#SourceConn = MySQLdb.connect(host='192.168.80.104',user='influx',passwd='influx1234',port=3306)
#TargetConn  = MySQLdb.connect(host='192.168.80.104',user='influx',passwd='influx1234',port=3306)

SourceConn = MySQLdb.connect(host='localhost',user='root',passwd='root',port=3306)
TargetConn = MySQLdb.connect(host='localhost',user='root',passwd='root',port=3306)

querySql = "SELECT MAX(EndID)-MIN(BeginID),SUM(flow) FROM `migrationTask_test` WHERE SourceTableName = %s AND DATE_FORMAT(EndTime,'%%y-%%m-%%d') = DATE_FORMAT(NOW(),'%%y-%%m-%%d')"
updateSql = "UPDATE ossean_monitors SET flow_num =%s , flow_rate = %s WHERE website=%s"

"""
def handleSql(sql,db,flag,*value):
    if flag == 'source':
        cur = SourceConn.cursor()
    elif flag == 'target':
        cur = TargetConn.cursor()
    count = cur.execute(sql,value)
    print count
    result = cur.
    pass
"""

def checkSourceDB(sql , value):
    cur = SourceConn.cursor()
    SourceConn.select_db("test_db")
    count = cur.execute(sql,value)
    print count
    logger.info(count)
    result = cur.fetchone()
    SourceConn.commit()
    return result

def updateTargetDB(sql,*value):
    cur = TargetConn.cursor()
    TargetConn.select_db("test_db")
    count = cur.execute(sql,value)
    TargetConn.commit()
    return count

def closeCon():
    SourceConn.close()
    TargetConn.close()

for name in sourceName:
    ans = checkSourceDB(querySql,name)
    dayFlow[name] = ans
    print name,'is',ans

    if dayFlow[name][1] == None:
        rate = 0.0
    else:
        rate = dayFlow[name][1]/dayFlow[name][0]
    dayFlowRate[name] = rate
    print rate

#main
for key,value in dayFlow.items():
    print key,"is",value
    if  value[0] == None:
        continue
    flow_num = int(value[0])
    flow_rate = dayFlowRate[key]
    condition_value = (flow_num,flow_rate,key)
    print condition_value
    logger.info(condition_value)
    updateTargetDB(updateSql,flow_num,flow_rate,key)

for key,value in dayFlowRate.items():
    print key,"is",value

closeCon()

