#! python3
# -*- coding=utf-8 -*-

from lxml import etree
import pymysql
import time
import sys


def create_tables():
    try:
        cursor.execute('drop table if exists NodeTag, WayTag, WayNode, Node, Way')
        cursor.execute('create table Node(\
                        NodeID INT UNSIGNED not null primary key,\
                        Lat double not null, INDEX (Lat),\
                        Lon double not null, INDEX (Lon),\
                        Pos point not null, SPATIAL INDEX (Pos),\
                        IsPOI boolean default FALSE\
                        ) ENGINE=MyISAM')
        cursor.execute('create table Way(\
                        WayID INT UNSIGNED not null primary key\
                        ) ENGINE=MyISAM')
        cursor.execute('create table NodeTag(\
                        NodeID INT UNSIGNED not null,\
                        K    varchar(200) not null, INDEX (k),\
                        V    varchar(200) not null, INDEX (V),\
                        foreign key(NodeID) references Node(NodeID)\
                        ) ENGINE=MyISAM')
        cursor.execute('create table WayTag(\
                        WayID INT UNSIGNED not null,\
                        K    varchar(200) not null, INDEX (k),\
                        V    varchar(200) not null, INDEX (V),\
                        foreign key(WayID) references Way(WayID)\
                        ) ENGINE=MyISAM')
        cursor.execute('create table WayNode(\
                        WayID INT UNSIGNED not null,\
                        NodeID INT UNSIGNED not null,\
                        OrderNum INT not null,\
                        foreign key(WayID) references Way(WayID),\
                        foreign key(NodeID) references Node(NodeID)\
                        ) ENGINE=MyISAM')
    except Exception as e:
        print(e)
        raise e
        connection.close()

    # cur.execute('drop database if exists ShanghaiOsm')
    # cur.execute('create database ShanghaiOsm default character set utf8 collate utf8_general_ci')
    # conn.select_db('ShanghaiOsm')


def disable_index(tables):
    cursor.execute('LOCK TABLES %s' % ', '.join(['`%s` WRITE' % table for table in tables]))
    for table in tables:
        cursor.execute('/*!40000 ALTER TABLE `%s` DISABLE KEYS */' % table)
        print('disable index for table %s done' % table)


def enable_index(tables):
    for table in tables:
        cursor.execute('/*!40000 ALTER TABLE `%s` ENABLE KEYS */' % table)
        print('enable index for table %s done' % table)
    cursor.execute('UNLOCK TABLES')

def create_index(tableANDkeys):
    for (table, key) in tableANDkeys:
        cursor.execute('CREATE INDEX %s ON %s (%s)'%(table+'_'+key.replace(',','_'), table, key))
        print('create index %s for table %s with key %s done' % (table+'_'+key.replace(',','_'), table, key))

def insert(table, command, format, values):
    try:
        formatted = [format % value for value in values]
        cursor.execute(command + ','.join(formatted))
        # print('insert %d to %d records' % )
        connection.commit()
    except Exception as e:
        connection.rollback()
        print(e)
        raise e
        connection.close()


def parse_and_insert(filename):
    tables = ['Node', 'Way', 'NodeTag', 'WayTag', 'WayNode']
    index_tables = [('WayNode', 'WayID'), ('WayNode', 'NodeID')]
    disable_index(tables)
    table_node = []
    count_node = 0
    table_node_tag = []
    count_node_tag = 0
    table_way = []
    count_way = 0
    table_way_tag = []
    count_way_tag = 0
    table_way_node = []
    count_way_node = 0

    for event, element in etree.iterparse(filename):
        if element.tag == 'node':
            # print(etree.tostring(element))
            node_id = int(element.get('id'))
            table_node.append((node_id,
                               float(element.get('lat')),
                               float(element.get('lon')),
                               'POINT(%s, %s)' % (element.get('lat'), element.get('lon'))))
            for tag in element.getchildren():
                # print(tag.get('k'))
                table_node_tag.append((node_id,
                                       connection.escape_string(tag.get('k')),
                                       connection.escape_string(tag.get('v'))))
            element.clear()
        elif element.tag == 'way':
            way_id = int(element.get('id'))
            table_way.append((way_id,))
            for tag in element.getchildren():
                if tag.tag == 'tag':
                    table_way_tag.append((way_id,
                                          connection.escape_string(tag.get('k')),
                                          connection.escape_string(tag.get('v'))))
            order = 0
            for nd in element.getchildren():
                if nd.tag == 'nd':
                    table_way_node.append((way_id,
                                           int(nd.get('ref')),
                                           order))
                    order += 1
            element.clear()
        if len(table_node) >= 10000:
            insert('Node', 'insert into Node(NodeID, Lat, Lon, Pos) values ', '(%d, %f, %f, %s)', table_node)
            count_node += len(table_node)
            print('insert table node %d' % count_node)
            sys.stdout.flush()
            table_node = []
        if len(table_node_tag) >= 10000:
            insert('NodeTag', 'insert into NodeTag(NodeID, K, V) values ', '(%d, \'%s\', \'%s\')', table_node_tag)
            count_node_tag += len(table_node_tag)
            print('insert table node_tag %d' % count_node_tag)
            sys.stdout.flush()
            table_node_tag = []
        if len(table_way) >= 10000:
            insert('Way', 'insert into Way(WayID) values ', '(%d)', table_way)
            count_way += len(table_way)
            print('insert table way %d' % count_way)
            sys.stdout.flush()
            table_way = []
        if len(table_way_tag) >= 10000:
            insert('WayTag', 'insert into WayTag(WayID, K, V) values ', '(%d, \'%s\', \'%s\')', table_way_tag)
            count_way_tag += len(table_way_tag)
            print('insert table way_tag %d' % count_way_tag)
            sys.stdout.flush()
            table_way_tag = []
        if len(table_way_node) >= 10000:
            insert('WayNode', 'insert into WayNode(WayID, NodeID, OrderNum) values ', '(%d, %d, %d)', table_way_node)
            count_way_node += len(table_way_node)
            print('insert table way_node %d' % count_way_node)
            sys.stdout.flush()
            table_way_node = []
    print('parse table way done')
    print('parse table way_tag done')
    print('parse table way_node done')
    enable_index(tables)
    create_index(index_tables)

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
    connection =  get_database_connection()
    cursor = connection.cursor()
    print('connected')
    create_tables()
    begin_time = time.time()
    print('loaded')
    parse_and_insert('shanghai_dump.osm')

    cursor.close()
    connection.close()
