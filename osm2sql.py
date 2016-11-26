#! python3
# -*- coding=utf-8 -*-

from lxml import etree
import pymysql
import time
import sys
import json


def create_tables():
    try:
        # cursor.execute('drop table if exists NodeTag, WayTag, WayNode, Node, Way')
        cursor.execute('drop table if exists WayNode, Node, Way')
        cursor.execute('create table Node(\
                        NodeID BIGINT not null primary key,\
                        Lat double not null, INDEX (Lat),\
                        Lon double not null, INDEX (Lon),\
                        Pos point not null, SPATIAL INDEX (Pos),\
                        TagData JSON not null, \
                        Name varchar(200), INDEX(Name), \
                        IsPOI boolean default FALSE\
                        ) ENGINE=MyISAM')
        cursor.execute('create table Way(\
                        WayID BIGINT not null primary key, \
                        TagData JSON not null, \
                        Name varchar(200), INDEX(Name) \
                        ) ENGINE=MyISAM')
        # cursor.execute('create table NodeTag(\
        #                 NodeID BIGINT not null,\
        #                 K    varchar(200) not null, INDEX (k),\
        #                 V    varchar(200) not null, INDEX (V),\
        #                 foreign key(NodeID) references Node(NodeID)\
        #                 ) ENGINE=MyISAM')
        # cursor.execute('create table WayTag(\
        #                 WayID BIGINT not null,\
        #                 K    varchar(200) not null, INDEX (k),\
        #                 V    varchar(200) not null, INDEX (V),\
        #                 foreign key(WayID) references Way(WayID)\
        #                 ) ENGINE=MyISAM')
        cursor.execute('create table WayNode(\
                        WayID BIGINT not null, INDEX(WayID),\
                        NodeID BIGINT not null, INDEX(NodeID),\
                        OrderNum INT not null,\
                        foreign key(WayID) references Way(WayID),\
                        foreign key(NodeID) references Node(NodeID)\
                        ) ENGINE=MyISAM')
    except Exception as e:
        print(e)
        raise e
        connection.close()


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


def insert(table, command, format, values):
    try:
        formatted = [format % value for value in values]
        # print(command + ','.join(formatted))
        cursor.execute(command + ','.join(formatted))
        # print('insert %d to %d records' % )
        connection.commit()
    except Exception as e:
        connection.rollback()
        print(e)
        raise e
        connection.close()


def parse_and_insert(filename):
    # tables = ['Node', 'Way', 'NodeTag', 'WayTag', 'WayNode']
    tables = ['Node', 'Way', 'WayNode']
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
            isPOI = False
            name = 'Null'
            tag_data = []
            for tag in element.getchildren():
                tag_data.append({tag.get('k'):
                                 tag.get('v')})
                if tag.get('k') == 'name':
                    name = connection.escape(tag.get('v'))
                    isPOI = True
            tag_data = connection.escape(json.dumps(tag_data))

            node_id = int(element.get('id'))
            table_node.append((node_id,
                               float(element.get('lat')),
                               float(element.get('lon')),
                               'POINT(%s, %s)' % (element.get('lat'), element.get('lon')),
                               tag_data,
                               name,
                               isPOI
            ))
            element.clear()

        elif element.tag == 'way':
            way_id = int(element.get('id'))
            tag_data = []
            name = 'Null'
            for tag in element.getchildren():
                if tag.tag == 'tag':
                    tag_data.append({tag.get('k'):
                                     tag.get('v')})
                    if tag.get('k') == 'name':
                        name = connection.escape(tag.get('v'))
            tag_data = connection.escape(json.dumps(tag_data))
                    
            table_way.append((way_id, tag_data, name))
            # table_way_tag.append((way_id,
            #                       connection.escape_string(tag.get('k')),
            #                       connection.escape_string(tag.get('v'))))
            order = 0
            for nd in element.getchildren():
                if nd.tag == 'nd':
                    table_way_node.append((way_id,
                                           int(nd.get('ref')),
                                           order))
                    order += 1
            element.clear()
        if len(table_node) >= 10000:
            insert('Node', 'insert into Node(NodeID, Lat, Lon, Pos, TagData, Name, IsPOI) values ', '(%d, %f, %f, %s, %s, %s, %d)', table_node)
            count_node += len(table_node)
            print('insert table node %d' % count_node)
            sys.stdout.flush()
            table_node = []
        # if len(table_node_tag) >= 10000:
        #     insert('NodeTag', 'insert into NodeTag(NodeID, K, V) values ', '(%d, \'%s\', \'%s\')', table_node_tag)
        #     count_node_tag += len(table_node_tag)
        #     print('insert table node_tag %d' % count_node_tag)
        #     sys.stdout.flush()
        #     table_node_tag = []
        if len(table_way) >= 10000:
            insert('Way', 'insert into Way(WayID, TagData, Name) values ', '(%d, %s, %s)', table_way)
            count_way += len(table_way)
            print('insert table way %d' % count_way)
            sys.stdout.flush()
            table_way = []
        # if len(table_way_tag) >= 10000:
        #     insert('WayTag', 'insert into WayTag(WayID, K, V) values ', '(%d, \'%s\', \'%s\')', table_way_tag)
        #     count_way_tag += len(table_way_tag)
        #     print('insert table way_tag %d' % count_way_tag)
        #     sys.stdout.flush()
        #     table_way_tag = []
        if len(table_way_node) >= 10000:
            insert('WayNode', 'insert into WayNode(WayID, NodeID, OrderNum) values ', '(%d, %d, %d)', table_way_node)
            count_way_node += len(table_way_node)
            print('insert table way_node %d' % count_way_node)
            sys.stdout.flush()
            table_way_node = []

    if len(table_node) > 0 :
        insert('Node', 'insert into Node(NodeID, Lat, Lon, Pos, TagData, Name, IsPOI) values ', '(%d, %f, %f, %s, %s, %s, %d)', table_node)
        count_node += len(table_node)
        print('insert table node %d' % count_node)
        sys.stdout.flush()
    if len(table_way) > 0 :
        insert('Way', 'insert into Way(WayID, TagData, Name) values ', '(%d, %s, %s)', table_way)
        count_way += len(table_way)
        print('insert table way %d' % count_way)
        sys.stdout.flush()
    if len(table_way_node) > 0 :
        insert('WayNode', 'insert into WayNode(WayID, NodeID, OrderNum) values ', '(%d, %d, %d)', table_way_node)
        count_way_node += len(table_way_node)
        print('insert table way_node %d' % count_way_node)
        sys.stdout.flush()
        
    print('parse table way done')
    print('parse table way_tag done')
    print('parse table way_node done')
    enable_index(tables)
    # create_index(index_tables)

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
