#! python3
# -*- coding=utf-8 -*-

import pymysql


if __name__ == '__main__':
    print('start')
    connection = pymysql.connect(host='127.0.0.1',
                                 user='dbproject',
                                 password='tycjytzp',
                                 db='ShanghaiOsm',
                                 charset='utf8mb4',
                                 port=3306,
                                 cursorclass=pymysql.cursors.DictCursor)
    cursor = connection.cursor()
    cursor.execute("SELECT NodeID, Lat, Lon from Node")
    Node2Pos = {i['NodeID']: (i['Lat'], i['Lon']) for i in cursor.fetchall()}
    cursor.execute("SELECT WayID, NodeID, OrderNum from WayNode")
    Way2Nodes = {i['NodeID']: (i['OrderNum'], i['NodeID']) for i in cursor.fetchall()}
