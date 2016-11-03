#! python3
# -*- coding=utf-8 -*-

from lxml import etree
import pymysql
import time


def create_tables():
    try:
        cursor.execute('drop table if exists NodeTag, WayTag, WayNode, Node, Way')
        cursor.execute('create table Node(\
                        NodeID INT UNSIGNED not null primary key,\
                        Lat double not null,\
                        Lon double not null,\
                        Point point,\
                        IsPOI boolean default FALSE\
                        )')
        cursor.execute('create table Way(\
                        WayID INT UNSIGNED not null primary key\
                        )')
        cursor.execute('create table NodeTag(\
                        NodeID INT UNSIGNED not null,\
                        K    varchar(255) not null,\
                        V    varchar(255) not null,\
                        foreign key(NodeID) references Node(NodeID)\
                        )')
        cursor.execute('create table WayTag(\
                        WayID INT UNSIGNED not null,\
                        K    varchar(255) not null,\
                        V    varchar(255) not null,\
                        foreign key(WayID) references Way(WayID)\
                        )')
        cursor.execute('create table WayNode(\
                        WayID INT UNSIGNED not null,\
                        NodeID INT UNSIGNED not null,\
                        OrderNum INT not null,\
                        foreign key(WayID) references Way(WayID),\
                        foreign key(NodeID) references Node(NodeID)\
                        )')
    except Exception as e:
        print(e)
        raise e
        connection.close()

    # cur.execute('drop database if exists ShanghaiOsm')
    # cur.execute('create database ShanghaiOsm default character set utf8 collate utf8_general_ci')
    # conn.select_db('ShanghaiOsm')


def insert(command, values):
    try:
        formatted = [connection.escape(value) for value in values]
        for i in range(0, len(formatted), 10000):
            cursor.execute(command + ','.join(formatted[i:i + 10000]))
            connection.commit()
            print('insert %d to %d records' % (i, i + 10000))
    except Exception as e:
        connection.rollback()
        print(e)
        raise e
        connection.close()


def parse_and_insert(xml):
    nodes = xml.findall('//node')
    ways = xml.findall('//way')
    # table node
    table_node = []
    for node in nodes:
        table_node.append((int(node.get('id')),
                           float(node.get('lat')),
                           float(node.get('lon'))))
    print('parse table node done')
    # table way
    table_way = []
    for way in ways:
        table_way.append((int(way.get('id')),))
    print('parse table way done')
    # table node_tag
    table_node_tag = []
    for node in nodes:
        node_id = int(node.get('id'))
        for tag in node.getchildren():
            table_node_tag.append((node_id,
                                   tag.get('k'),
                                   tag.get('v')))
    print('parse table node_tag done')
    # table way_tag
    table_way_tag = []
    for way in ways:
        way_id = int(way.get('id'))
        for tag in way.getchildren():
            if tag.tag == 'tag':
                table_way_tag.append((way_id,
                                      tag.get('k'),
                                      tag.get('v')))
    print('parse table way_tag done')
    # table way_node
    table_way_node = []
    for way in ways:
        way_id = int(way.get('id'))
        order = 0
        for nd in way.getchildren():
            if nd.tag == 'nd':
                table_way_node.append((way_id,
                                       int(nd.get('ref')),
                                       order))
                order += 1
    print('parse table way_node done')
    insert('insert into Node(NodeID, Lat, Lon) values ', table_node)
    print('insert into Node done')
    insert('insert into Way(WayID) values ', table_way)
    print('insert into Way done')
    insert('insert into NodeTag(NodeID, K, V) values ', table_node_tag)
    print('insert into NodeTag done')
    insert('insert into WayTag(WayID, K, V) values ', table_way_tag)
    print('insert into WayTag done')
    insert('insert into WayNode(WayID, NodeID, OrderNum) values ', table_way_node)
    print('insert into WayNode done')


if __name__ == '__main__':
    connection = pymysql.connect(host='127.0.0.1',
                                 user='dbproject',
                                 password='tycjytzp',
                                 db='ShanghaiOsm',
                                 charset='utf8mb4',
                                 port=3306,
                                 cursorclass=pymysql.cursors.Cursor)
    cursor = connection.cursor()
    create_tables()
    begin_time = time.time()
    XML = etree.parse(open('shanghai_dump.osm', encoding='utf-8'))
    parse_and_insert(XML)

    cursor.close()
    connection.close()
