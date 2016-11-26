#! python3
# -*- coding=utf-8 -*-

from lxml import etree
import pymysql
import time
import sys

if __name__ == '__main__':
    print('start')
    connection = pymysql.connect(host='127.0.0.1',
                                 user='root',
                                 password='5130309773',
                                 db='ShanghaiOsm',
                                 charset='utf8mb4',
                                 port=3306,
                                 cursorclass=pymysql.cursors.DictCursor)
    cursor = connection.cursor()
    cursor.execute("SELECT NodeID, Lat, Lon from Node")
    Node2Pos = {i['NodeID']: (i['Lat'], i['Lon']) for i in cursor.fetchall()}
    Way2Nodes = {}
    cursor.execute("SELECT WayID, NodeID, OrderNum from WayNode")
    for i in cursor.fetchall():
        if i['WayID'] not in Way2Nodes:
            Way2Nodes[i['WayID']] = []
        Way2Nodes[i['WayID']].append((i['OrderNum'], i['NodeID']))
    R = 6378137
    virtual_nodes = []
    way_virtual_node = []
    count = 1
    for k, v in Way2Nodes.items():
        for i in range(len(v) - 1):
            lat0 = Node2Pos[v[i][1]][0] / 180 * pi
            lat1 = Node2Pos[v[i + 1][1]][0] / 180 * pi
            lon0 = Node2Pos[v[i][1]][1] / 180 * pi
            lon1 = Node2Pos[v[i + 1][1]][1] / 180 * pi
            a = lat0 - lat1
            b = lon0 - lon1
            dis = 2 * asin(sqrt(sin(a/2)**2+cos(lat0)*cos(lat1)*sin(b/2)**2)) * R
            if dis > 100:
                step_lat = (lat1 - lat0) / (dis // 100)
                step_lon = (lon1 - lon0) / (dis // 100)
                for j in range(int(dis // 100)):
                    virtual_nodes.append((-count, Node2Pos[v[i][1]][0] + step_lat * (j + 1), Node2Pos[v[i][1]][1] + step_lon * (j + 1)))
                    way_virtual_node.append((k, -count))
                    count += 1
    virtual_nodes = ['({0:d},{1:f},{1:f},POINT({1:f}, {1:f}),\'[]\')'.format(*i) for i in virtual_nodes]
    way_virtual_node = ['(%d,%d,-1)' % i for i in way_virtual_node]
    cursor.execute('LOCK TABLES Node WRITE, WayNode WRITE')
    cursor.execute('/*!40000 ALTER TABLE `Node` DISABLE KEYS */')
    cursor.execute('/*!40000 ALTER TABLE `Waynode` DISABLE KEYS */')
    for i in range(0, len(virtual_nodes), 10000):
        cursor.execute('INSERT into Node(NodeID,Lat,Lon,Pos,TagData) values %s' % ','.join(virtual_nodes[i:i+10000]))
        connection.commit()
    for i in range(0, len(way_virtual_node), 10000):
        cursor.execute('INSERT into WayNode(WayID,NodeID,OrderNum) values %s' % ','.join(way_virtual_node[i:i+10000]))
        connection.commit()
    cursor.execute('/*!40000 ALTER TABLE `Node` ENABLE KEYS */')
    cursor.execute('/*!40000 ALTER TABLE `WayNode` ENABLE KEYS */')
    cursor.execute('UNLOCK TABLES')
    cursor.close()
    connection.close()
