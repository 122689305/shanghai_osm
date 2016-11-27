#! python3
# -*- coding=utf-8 -*-
import pymysql
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

    # set default node_id as test
    node_id = 3949788227
    # TODO: accept the query as longitude and latitude or as the id of the node

    cursor.execute('select * from (select WayID from WayNode where NodeID=%d) AS tmp natural join Way'%node_id)
    way_list = cursor.fetchall()
    print('%d ways'%len(way_list))
    if len(way_list) > 1:
        print('crossed')
    for r in way_list:
        for key, value in r.items():
            print('%s\t%s'%(key,value))
        print('')

    # TODO: do we need to attach the tag info to the way?
    # Done

    print(datetime.datetime.now() - begin_mtime)



'''
mysql> select cnt, NodeID from (select count(*) as cnt, NodeID from WayNode group by NodeID) as cnt_table order by cnt desc limit 10;
+-----+------------+
| cnt | NodeID     |
+-----+------------+
|  16 | 3949788227 |
|  14 | 3817255212 |
|  14 | 4013081482 |
|  14 | 3817255218 |
|  14 | 3817255200 |
|  14 | 3817255226 |
|  11 | 4015124624 |
|  11 | 4015124634 |
|  11 | 4017878505 |
|  11 | 4015124631 |
+-----+------------+
10 rows in set (22.59 sec)
'''