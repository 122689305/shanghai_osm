# -*- coding=utf-8 -*-
import pymysql
import datetime
import sys

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

    # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    # !!!
    # TODO: DISTANCE CALCULATION is based on Euclidean Space, not sphere space
    # !!!
    # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

    location = {'Lat':31.218899, 'Lon':121.413458}
    units = 111.045
    INITIAL_RADIUS = 0.1 # km with units as 111.045
    SEARCH_EXPANSION_RATE = 2 # expand the search radius with times as this number
    MAX_SEARCH_CNT = 20 # max times of tries to search
    # radius = 0.1 # km with units as 111.045
    # radius = 10 # mile with units as 69.0

    search_cnt = 0
    while True:
      search_cnt += 1
      if search_cnt > MAX_SEARCH_CNT:
        break

      radius = INITIAL_RADIUS * SEARCH_EXPANSION_RATE ** (search_cnt-1)
      cursor.execute('SET @latpoint=%f, @longpoint=%f, @r=%f, @units=%f'%(location['Lat'], location['Lon'], radius, units) )
      cursor.execute('''
      SELECT NodeID FROM (
        SELECT NodeID ,
                @units * DEGREES( ACOS(
                           COS(RADIANS(@latpoint)) 
                         * COS(RADIANS(Lat)) 
                         * COS(RADIANS(@longpoint) - RADIANS(Lon)) 
                         + SIN(RADIANS(@latpoint)) 
                         * SIN(RADIANS(Lat)))) AS distance
        FROM Node
        WHERE MbrContains(ST_GeomFromText (
                CONCAT('LINESTRING(',
                      @latpoint-(@r/@units),' ',
                      @longpoint-(@r /(@units* COS(RADIANS(@latpoint)))),
                      ',', 
                      @latpoint+(@r/@units) ,' ',
                      @longpoint+(@r /(@units * COS(RADIANS(@latpoint)))),
                      ')')),  Pos)
          AND IsPOI = True
        ) AS d
      WHERE distance < @r
      ''')
      node_list = cursor.fetchall()
      print('search Lat:%f Lon:%f with raidus %f km: %d nodes'%(location['Lat'], location['Lon'], radius, len(node_list)))
      if len(node_list) > 0:
        node_id_list = [str(node['NodeID']) for node in node_list]
        cursor.execute('SELECT DISTINCT WayID FROM WayNode WHERE NodeID in (%s)'%(','.join(node_id_list)))
        way_list = cursor.fetchall()
        way_distance = {}
        if len(way_list) > 0:
          for way in way_list:
            way_id = way['WayID']
            cursor.execute('SELECT Lat, Lon FROM Node NATURAL JOIN (SELECT NodeID, OrderNum FROM WayNode WHERE WayID = %s) as tmp ORDER BY OrderNum ASC'%way_id)
            node_list = cursor.fetchall()
            query = '''
              SET @way=ST_GeomFromText("LineString(%s)"), 
                  @pos=POINT(%s);
              '''%(','.join(\
                ['%f %f'\
                    %(node['Lat'], node['Lon'])\
                  for node in node_list]),\
                '%f, %f'%(location['Lat'], location['Lon']))
            cursor.execute(query)
            cursor.execute('SELECT ST_DISTANCE(@way, @pos) AS distance;')
            distance = cursor.fetchone()
            way_distance[way_id] = distance['distance']
          way_distance = sorted(way_distance.items(), key=lambda d: d[1])
          closest_way = way_distance[0]
          print(way_distance[:10])
          break    
      else:
        continue

    print('closest way is %d with distance %f'%(closest_way[0], closest_way[1]))
    print(datetime.datetime.now() - begin_mtime)

'''
mysql> SET @g1 = ST_GeomFromText('LineString(0 0, 0 1, 1 1, 0 1)'), @g2 = Point(2,1);
Query OK, 0 rows affected (0.00 sec)

mysql> SELECT ST_DISTANCE(@g1, @g2);
+-----------------------+
| ST_DISTANCE(@g1, @g2) |
+-----------------------+
|                     1 |
+-----------------------+
1 row in set (0.00 sec)
'''


'''
search Lat:31.218899 Lon:121.413458 with raidus 0.100000 km: 91 nodes
search Lat:31.218899 Lon:121.413458 with raidus 0.200000 km: 398 nodes
[(97546587, 0.0017765009773551153)]
closest way is 97546587 with distance 0.001777
0:00:00.080000
'''