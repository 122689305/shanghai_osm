# -*- coding=utf-8 -*-
import pymysql
import datetime
import json

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

    boundry = {'x1':31, 'y1':120, 'x2':31.2, 'y2':121}
    # radius = 10 # mile with units as 69.0
    f_osm = open('part_shanghai_dump.osm', 'w')

    f_osm.write('''<?xml version='1.0' encoding='UTF-8'?>
<osm version="0.6" generator="SJTU-2013-D19-527 2333">
\t<bounds minlon="%f" minlat="%f" maxlon="%f" maxlat="%f" origin="Osmosis 0.44.1"/>
'''%(boundry['x1'], boundry['y1'], boundry['x2'], boundry['y2']))


    query = '''
      SELECT DISTINCT NodeID, Lat, Lon, TagData FROM WayNode NATURAL JOIN Node WHERE MbrContains(ST_GeomFromText('LineString(%f %f, %f %f)'), Pos) AND NodeID > 0 ORDER BY IsPOI DESC
      '''%(boundry['x1'], boundry['y1'], boundry['x2'], boundry['y2'])
    cursor.execute(query)
    node_list = cursor.fetchall()
    for node in node_list:
      # print node
      node_id = node['NodeID']
      node_lat = node['Lat']
      node_lon = node['Lon']
      node_tagdata = json.loads(node['TagData'])
      f_osm.write('\t<node id="%s" lat="%s" lon="%s">\n'%(node_id, node_lat, node_lon))
      for tag_k, tag_v in node_tagdata.items():
        f_osm.write('\t\t<tag k="%s" v="%s"/>\n'%(tag_k, tag_v))
      f_osm.write('\t</node>\n')
      f_osm.flush()

    cursor.execute('''
      SELECT DISTINCT WayID FROM WayNode NATURAL JOIN Node WHERE MbrContains(ST_GeomFromText('LineString(%f %f, %f %f)'), Pos) AND NodeID > 0
      '''%(boundry['x1'], boundry['y1'], boundry['x2'], boundry['y2']))
    way_list = cursor.fetchall()
    for way in way_list:
      way_id = way['WayID']
      cursor.execute('SELECT TagData FROM Way WHERE WayID = %s'%way_id)
      way_tagdata = cursor.fetchone()
      way_tagdata = json.loads(way_tagdata['TagData'])
      f_osm.write('\t<way id="%s">\n'%way_id)
      cursor.execute('SELECT NodeID FROM WayNode WHERE WayID = %s AND NodeID > 0'%way_id)
      node_list = cursor.fetchall()
      for node in node_list:
        f_osm.write('\t\t<nd ref="%s"/>\n'%node['NodeID'])
      for tag_k, tag_v in way_tagdata.items():
        f_osm.write('\t\t<tag k="%s" v="%s"/>\n'%(tag_k.encode('utf-8'), tag_v.encode('utf-8')))
      f_osm.write('\t</way>\n')
      f_osm.flush()
    f_osm.write('</osm>')

    print(datetime.datetime.now() - begin_mtime)

'''
<?xml version='1.0' encoding='UTF-8'?>
<osm version="0.6" generator="Osmosis 0.44.1">
  <bounds minlon="-180.00000" minlat="-90.00000" maxlon="180.00000" maxlat="90.00000" origin="Osmosis 0.44.1"/>
  <node id="1" version="1" timestamp="2016-05-21T09:37:55Z" changeset="-1" lat="31.2268644" lon="121.5310825">
    <tag k="name" v="美亚财产保险有限公司"/>
    <tag k="poitype" v="金融"/>
    <tag k="source" v="baidu"/>
  </node>
  <way id="39659503" version="-1" timestamp="1969-12-31T23:59:59Z" changeset="-1">
    <nd ref="475427814"/>
    <nd ref="475426566"/>
    <tag k="ref" v="S106"/>
    <tag k="name" v="沪太路"/>
    <tag k="oneway" v="yes"/>
    <tag k="highway" v="primary"/>
    <tag k="name:en" v="Hutai Road"/>
  </way>
</osm>
'''

'''
mysql> select min(Lat), max(Lat), min(Lon), max(Lon) from Node;
+-----------+-----------+-----------+------------+
| min(Lat)  | max(Lat)  | min(Lon)  | max(Lon)   |
+-----------+-----------+-----------+------------+
| 30.614001 | 31.787997 | 30.614005 | 121.972899 |
+-----------+-----------+-----------+------------+
1 row in set (0.03 sec)

mysql> select min(Lat), max(Lat), min(Lon), max(Lon) from Node where NodeID > 0;
+-----------+-----------+------------+------------+
| min(Lat)  | max(Lat)  | min(Lon)   | max(Lon)   |
+-----------+-----------+------------+------------+
| 30.614001 | 31.787997 | 119.787003 | 121.972899 |
+-----------+-----------+------------+------------+

mysql> select count(*) from Node where Lon <  100;
+----------+
| count(*) |
+----------+
|   581360 |
+----------+
1 row in set (1.87 sec)

mysql> select count(*) from Node where Lon <  100 and NodeID > 0;
+----------+
| count(*) |
+----------+
|        0 |
+----------+
1 row in set (0.80 sec)
'''

'''
mysql> select count(*) from Node where ST_Y(POS) > 120.1 and ST_Y(POS) < 120.2 and ST_X(POS) > 30.7 and ST_X(POS) < 30.8 and NodeID > 0;
+----------+
| count(*) |
+----------+
|     1913 |
+----------+
1 row in set (1.12 sec)

mysql> select count(*) from Node where Lon > 120.1 and Lon < 120.2 and Lat > 30.7 and Lat < 30.8 and NodeID > 0;
+----------+
| count(*) |
+----------+
|     1913 |
+----------+
1 row in set (0.18 sec)

mysql> select count(*) from Node where MbrContains(ST_GeomFromText('LineString(30.7 120.1, 30.8 120.2)'), Pos);
+----------+
| count(*) |
+----------+
|     1913 |
+----------+
1 row in set (0.01 sec)
'''