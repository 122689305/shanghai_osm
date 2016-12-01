#! python3
# -*- coding=utf-8 -*-
import pymysql
import datetime
import json
import jieba
from math import *


def init():
    f = open('config/default.ini')
    (host, port, user, password) = tuple([word.strip() for word in f.readlines()])
    port = int(port)
    print(host, port, user, password)
    global connection, cursor, units
    units = 111.045
    connection = pymysql.connect(host=host,
                                 user=user,
                                 password=password,
                                 db='ShanghaiOsm',
                                 charset='utf8mb4',
                                 port=port,
                                 cursorclass=pymysql.cursors.DictCursor)
    cursor = connection.cursor()


def query1(node_id):
    '''
    return json, [{}, {}, {}], each {} is a node
    '''
    print(node_id)
    cursor.execute('select * from (select WayID from WayNode where NodeID=%d) AS tmp natural join Way' % node_id)
    way_list = cursor.fetchall()
    print(way_list)
    return json.dumps(way_list)


def query2(way_id):
    '''
    return json, [{}, {}, {}], each {} is a way
    '''
    cursor.execute('SELECT NodeID, Lat, Lon, TagData FROM (SELECt NodeID,OrderNum FROM WayNode WHERE WayID=%d) AS tmp NATURAL JOIN Node ORDER BY OrderNum ASC' % way_id)
    node_list = cursor.fetchall()
    return json.dumps(node_list)


def query3(way_name):
    '''
    return json, [{}, {}, {}], each {} is a way
    '''
    way_name_list = [word for word in jieba.cut(way_name) if not word.strip() == '']
    way_name_like = ' and '.join(['Name like \'%%%s%%\'' % way_name_part for way_name_part in way_name_list])
    query = 'select * from Way where %s' % way_name_like
    cursor.execute(query)
    way_list = cursor.fetchall()
    return json.dumps(way_list)


def dist(pos1, pos2):
    lat0 = pos1['Lat'] / 180 * pi
    lat1 = pos2['Lat'] / 180 * pi
    lon0 = pos1['Lon'] / 180 * pi
    lon1 = pos2['Lon'] / 180 * pi
    a = (lat0 - lat1) / 2
    b = (lon0 - lon1) / 2
    return 2 * asin(sqrt(sin(a) * sin(a) + cos(lat0) * cos(lat1) * sin(b) * sin(b))) * units


def query4(lat, lon, r):
    '''
    return json, [{}, {}, {}], each {} is a node
    '''
    pos = {'Lat': lat, 'Lon': lon}
    bound = (pos['Lat'] - r / units, pos['Lat'] + r / units, pos['Lon'] - r / units, pos['Lon'] + r / units)
    cursor.execute("SELECT * from Node where NodeID >= 0 and Lat >= %f and Lat <= %f and Lon >= %f and Lon < %f AND IsPOI = True" % bound)
    node_list = [i for i in cursor.fetchall() if dist(i, pos) < r]
    node_list.sort(key=lambda x: dist(x, {'Lat': 31.218899, 'Lon': 121.413458}))
    return json.dumps(node_list)


def query5(lat, lon):
    '''
    return json, actually only one dict, which represents a way
    '''
    pos = {'Lat': lat, 'Lon': lon}
    INITIAL_RADIUS = 0.1  # km with units as 111.045
    SEARCH_EXPANSION_RATE = 2  # expand the search radius with times as this number
    MAX_SEARCH_CNT = 20  # max times of tries to search

    search_cnt = 0
    while True:
        search_cnt += 1
        if search_cnt > MAX_SEARCH_CNT:
            break

        r = INITIAL_RADIUS * SEARCH_EXPANSION_RATE ** (search_cnt - 1)
        bound = (pos['Lat'] - r / units, pos['Lat'] + r / units, pos['Lon'] - r / units, pos['Lon'] + r / units)
        cursor.execute("SELECT * from Node where Lat >= %f and Lat <= %f and Lon >= %f and Lon < %f" % bound)
        node_list = cursor.fetchall()
        if len(node_list) > 0:
            node_id_list = [str(node['NodeID']) for node in node_list]
            cursor.execute('SELECT WayID FROM WayNode WHERE NodeID in (%s)' % (','.join(node_id_list)))
            way_list = set(i['WayID'] for i in cursor.fetchall())
            way_distance = {}
            if len(way_list) > 0:
                for way_id in way_list:
                    cursor.execute('SELECT Lat, Lon FROM Node NATURAL JOIN (SELECT NodeID, OrderNum FROM WayNode WHERE WayID = %s and NodeID >= 0) as tmp ORDER BY OrderNum ASC' % way_id)
                    node_list = cursor.fetchall()
                    query = '''
              SET @way=ST_GeomFromText("LineString(%s)"), 
                  @pos=POINT(%s);
              ''' % (','.join(
                        ['%f %f'
                            % (node['Lat'], node['Lon'])
                         for node in node_list]),
                        '%f, %f' % (pos['Lat'], pos['Lon']))
                    cursor.execute(query)
                    cursor.execute('SELECT ST_DISTANCE(@way, @pos) AS distance;')
                    distance = cursor.fetchone()
                    way_distance[way_id] = distance['distance']
                way_distance = sorted(way_distance.items(), key=lambda d: d[1])
                closest_way = way_distance[0]
                print("closest_way", closest_way)
                cursor.execute('SELECT * from Way where WayID=%s' % closest_way[0])
                way = cursor.fetchone()
                print("way", way)
                return json.dumps(way)


def query6(lat0, lon0, lat1, lon1):
    '''
    return the filename of the dumped file
    '''
    bound = (lat0, lon0, lat1, lon1)
    filename = 'dump-%s.osm' % str(datetime.datetime.now()).replace(' ', '_').replace(':', '_')
    f_osm = open(filename, 'w')
    f_osm.write('''<?xml version='1.0' encoding='UTF-8'?>
<osm version="0.6" generator="SJTU-2013-D19-527 2333">
\t<bounds minlon="%f" minlat="%f" maxlon="%f" maxlat="%f" origin="Osmosis 0.44.1"/>
''' % bound)
    cursor.execute("SELECT DISTINCT NodeID, Lat, Lon, TagData FROM Node WHERE NodeID >= 0 and MbrContains(ST_GeomFromText('LineString(%f %f, %f %f)'), Pos)" % bound)
    node_list = cursor.fetchall()
    for node in node_list:
        # print node
        node_id = node['NodeID']
        node_lat = node['Lat']
        node_lon = node['Lon']
        node_tagdata = json.loads(node['TagData'])
        f_osm.write('\t<node id="%s" lat="%s" lon="%s">\n' % (node_id, node_lat, node_lon))
        for tag_k, tag_v in node_tagdata.items():
            f_osm.write('\t\t<tag k="%s" v="%s"/>\n' % (tag_k.encode('utf-8'), tag_v.encode('utf-8')))
        f_osm.write('\t</node>\n')
        # f_osm.flush()
    cursor.execute('''
      SELECT WayID from (select NodeID FROM Node WHERE MbrContains(ST_GeomFromText('LineString(%f %f, %f %f)'), Pos)) as tmp natural join WayNode
      ''' % bound)
    way_list = set(i['WayID'] for i in cursor.fetchall())
    for way_id in way_list:
        cursor.execute('SELECT TagData FROM Way WHERE WayID = %s' % way_id)
        way_tagdata = cursor.fetchone()
        way_tagdata = json.loads(way_tagdata['TagData'])
        f_osm.write('\t<way id="%s">\n' % way_id)
        cursor.execute('SELECT NodeID FROM WayNode WHERE WayID = %s AND NodeID > 0' % way_id)
        node_list = cursor.fetchall()
        for node in node_list:
            f_osm.write('\t\t<nd ref="%s"/>\n' % node['NodeID'])
        for tag_k, tag_v in way_tagdata.items():
            f_osm.write('\t\t<tag k="%s" v="%s"/>\n' % (tag_k.encode('utf-8'), tag_v.encode('utf-8')))
        f_osm.write('\t</way>\n')
        f_osm.flush()
    print(23333,filename)
    return filename
