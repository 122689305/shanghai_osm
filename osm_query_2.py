#! python3
# -*- coding=utf-8 -*-
import pymysql
import datetime


def get_database_connection():
    f = open('config/default.ini')
    (host, port, user, password) = tuple(
        [word.strip() for word in f.readlines()])
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
    connection = get_database_connection()
    cursor = connection.cursor()
    print('connected')

    # set default WayID as test
    WayID = 234483672
    # TODO: accept the query as longitude and latitude or as the id of the way

    cursor.execute('SELECT NodeID, Lat, Lon, TagData FROM (SELECt NodeID,OrderNum FROM WayNode WHERE WayID=%d) AS tmp NATURAL JOIN Node ORDER BY OrderNum ASC' % WayID)
    node_list = cursor.fetchall()
    print('%d nodes' % len(node_list))
    for r in node_list:
        for key, value in r.items():
            print('%s\t%s' % (key, value))
        print('')

    # TODO: do we need to attach the tag info to the way?
    # Done

    print(datetime.datetime.now() - begin_mtime)


'''
mysql> select cnt, WayID from (select count(*) as cnt, WayID from WayNode where nodeid > 0 group by WayID) as cnt_table order by cnt desc limit 10;
+------+-----------+
| cnt  | WayID     |
+------+-----------+
| 1661 | 234483672 |
| 1655 | 315526620 |
| 1385 | 234063306 |
| 1320 | 293268375 |
| 1198 | 370259506 |
| 1137 | 110408632 |
| 1073 | 243606024 |
|  918 | 259877120 |
|  889 |   4536635 |
|  861 | 334982626 |
+------+-----------+
10 rows in set (0.80 sec)

mysql> select cnt, WayID from (select count(*) as cnt, WayID from WayNode group by WayID) as cnt_table order by cnt desc limit 10;
+------+-----------+
| cnt  | WayID     |
+------+-----------+
| 2567 | 234483672 |
| 2152 | 370259506 |
| 2039 | 315526620 |
| 1392 | 234063306 |
| 1375 | 293268375 |
| 1143 | 110408632 |
| 1133 | 243557475 |
| 1075 | 243606024 |
| 1072 | 259877120 |
| 1027 | 364661234 |
+------+-----------+
10 rows in set (0.53 sec)
'''
