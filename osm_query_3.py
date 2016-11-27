#! python3
# -*- coding=utf-8 -*-
import pymysql
import jieba
import datetime

def get_database_connection():
    f = open('config/default.ini')
    (host, port, user, password) = tuple([word.strip() for word in f.readlines()])
    port = int(port)
    print (host, port, user, password)
    return pymysql.connect(host=host,
                             user=user,
                             password=password,
                             db='ShanghaiOsm',
                             charset='utf8mb4',
                             port=port,
                             cursorclass=pymysql.cursors.DictCursor)

if __name__ == '__main__':
    print('start')
    begin_mtime = datetime.datetime.now()
    connection =  get_database_connection()
    cursor = connection.cursor()
    print('connected')

    # set default way_name as test
    way_name_full = u'上海浦东国际机场1号航站楼 Shanghai Pudong International Airport Terminal 1'
    way_name_part = u'浦东机场 航站楼1号'
    way_name_part = u'航站楼'
    way_name = way_name_part
    way_name_list = [word for word in jieba.cut(way_name) if not word.strip() == '']
    print(way_name_list)
    # TODO: accept the query as longitude and latitude and show a list of possible ways

    way_name_like = ' and '.join(['Name like \'%%%s%%\''%way_name_part for way_name_part in way_name_list ])
    query = 'select * from Way where %s'%way_name_like
    print(query)
    cursor.execute(query)
    # TODO: show more tags of the way_id returned
    # Done
    way_list = cursor.fetchall()
    print('%d ways'%len(way_list))
    for r in way_list:
        for key, value in r.items():
            print('%s\t%s'%(key,value))
        print('')

    # TODO: do we need to attach the tag info to the way?
    # Done

    print(datetime.datetime.now() - begin_mtime)


'''
mysql> select WayID,Name from Way where not Name='' and length(Name) > 10 order by length(Name) desc limit 10;
+-----------+----------------------------------------------------------------------------+
| WayID     | Name                                                                       |
+-----------+----------------------------------------------------------------------------+
|  41128791 | 上海浦东国际机场1号航站楼 Shanghai Pudong International Airport Terminal 1 |
|  39023151 | 上海浦东国际机场2号航站搂 Shanghai Pudong International Airport Terminal 2 |
|  44024896 | Shanghai Science and Technology Museum Administrative Office Building      |
| 351538813 | 松江出入境检验检疫局驻松江出口加工区办事处                                 |
| 365667805 | HVDC Hubei-Shanghai / HVDC Gezhouba-Shanghai - electrode line              |
| 365689205 | HVDC Gezhouba-Shanghai/ HVDC Hubei-Shanghai - electrode line               |
| 295598908 | （在建）浦江社区卫生服务中心（浦航分院）                                   |
| 114518345 | 上海市绿色化学与化工过程绿色化重点实验室                                   |
| 328312315 | 上海迪士尼乐园 Shanghai Disneyland-Opening 2016/16/6                       |
| 358577511 | 江苏省苏州实验中学-南京大学苏州附属中学                                    |
+-----------+----------------------------------------------------------------------------+

mysql> select WayID,Name from Way where Name like '%浦东国际机场%';
+-----------+----------------------------------------------------------------------------+
| WayID     | Name                                                                       |
+-----------+----------------------------------------------------------------------------+
|  39023151 | 上海浦东国际机场2号航站搂 Shanghai Pudong International Airport Terminal 2 |
|  41128791 | 上海浦东国际机场1号航站楼 Shanghai Pudong International Airport Terminal 1 |
|  68744828 | 五洲大道/绕城高速/浦东国际机场/崇明                                        |
|  68744886 | 五洲大道/绕城高速/浦东国际机场/崇明                                        |
| 230069192 | 浦东国际机场                                                               |
+-----------+----------------------------------------------------------------------------+
5 rows in set (0.06 sec)


mysql> select NodeID,Name from Node where Name like '%浦东国际机场%';
+------------+-------------------------------------+
| NodeID     | Name                                |
+------------+-------------------------------------+
|      28319 | 7天连锁酒店(上海浦东国际机场店)     |
|      28419 | 城市便捷酒店(上海浦东国际机场店)    |
|      48414 | 上海浦东国际机场                    |
|     186495 | 浦东国际机场进出口公司              |
|   26609107 | 上海浦东国际机场                    |
|  826203191 | 五洲大道/绕城高速/浦东国际机场/崇明 |
| 3800386161 | 浦东国际机场                        |
| 3800386162 | 浦东国际机场                        |
+------------+-------------------------------------+
8 rows in set (1.39 sec)
'''