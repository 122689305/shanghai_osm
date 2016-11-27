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

    location = {'Lat':31.218899, 'Lon':121.413458}
    radius = 10 # km with units as 111.045
    # radius = 10 # mile with units as 69.0

    cursor.execute('SET @latpoint=%f, @longpoint=%f, @r=%f, @units=111.045'%(location['Lat'], location['Lon'], radius) )
    cursor.execute('''
      SELECT NodeID, Lat, Lon, Name, TagData FROM (
        SELECT NodeID, Lat, Lon, Name, TagData,
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
        ) as d
      WHERE distance < @r
      ORDER BY distance DESC
      ''')

    node_list = cursor.fetchall()
    print('search Lat:%f Lon:%f with raidus %f km'%(location['Lat'], location['Lon'], radius))
    print('%d nodes'%len(node_list))
    # for r in node_list:
    #     for key, value in r.items():
    #         print('%s\t%s'%(key,value))
    #     print('')

    print(datetime.datetime.now() - begin_mtime)

'''
mysql> select Lat,Lon from Node where NodeID = 3949788227;
+-----------+------------+
| Lat       | Lon        |
+-----------+------------+
| 31.218899 | 121.413458 |
+-----------+------------+
1 row in set (0.00 sec)
'''



'''
Lat: 31.218899
Lon: 121.413458

radius: 0.03 km
speed: 0 ms
nodes: 2 nodes

radius: 0.1 km
speed: 0.134 s
size: 91 nodes

radius: 1 km 
speed: 0.484 s
size: 7391 nodes

radius: 10 km
speed: 25.558 s
size: 369236 nodes
'''

'''
optimized query (with having and predefined variables):

Lat: 31.218899
Lon: 121.413458

radius: 0.03 km
speed: 0.044 ms
nodes: 2 nodes

radius: 0.1 km
speed: 0.053 s
size: 91 nodes

radius: 1 km 
speed: 0.657 s
size: 7391 nodes

radius: 10 km
speed: 49.272 s
size: 369236 nodes
'''

'''
optimized query (with predefined variables):

Lat: 31.218899
Lon: 121.413458

radius: 0.03 km
speed: 0.014 ms
nodes: 2 nodes

radius: 0.1 km
speed: 0.03 s
size: 91 nodes

radius: 1 km 
speed: 0.756 s
size: 7391 nodes

radius: 10 km
speed: 26.954 s
size: 369236 nodes
'''

"""
There are no geospatial extension functions in MySQL supporting latitude / longitude distance computations.

You're asking for proximity circles on the surface of the earth. You mention in your question that you have lat/long values for each row in your flags table, and also universal transverse Mercator (UTM) projected values in one of several different UTM zones. If I remember my UK Ordnance Survey maps correctly, UTM is useful for locating items on those maps.

It's a simple matter to compute the distance between two points in the same zone in UTM: the Cartesian distance does the trick. But, when points are in different zones, that computation doesn't work.

Accordingly, for the application described in your question, it's necessary to use the Great Circle Distance, which is computed using the haversine or another suitable formula.

MySQL, augmented with geospatial extensions, supports a way to represent various planar shapes (points, polylines, polygons, and so forth) as geometrical primitives. MySQL 5.6 implements an undocumented distance function st_distance(p1, p2). However, this function returns Cartesian distances. So it's entirely unsuitable for latitude and longitude based computations. At temperate latitudes a degree of latitude subtends almost twice as much surface distance (north-south) as a degree of longitude(east-west), because the latitude lines grow closer together nearer the poles.

So, a circular proximity formula needs to use genuine latitude and longitude.

In your application, you can find all the flags points within ten statute miles of a given latpoint,longpoint with a query like this:

 SELECT id, coordinates, name, r,
        units * DEGREES( ACOS(
                   COS(RADIANS(latpoint)) 
                 * COS(RADIANS(X(coordinates))) 
                 * COS(RADIANS(longpoint) - RADIANS(Y(coordinates))) 
                 + SIN(RADIANS(latpoint)) 
                 * SIN(RADIANS(X(coordinates))))) AS distance
   FROM flags
   JOIN (
        SELECT 42.81  AS latpoint,  -70.81 AS longpoint, 
               10.0 AS r, 69.0 AS units
        ) AS p ON (1=1)
  WHERE MbrContains(GeomFromText (
        CONCAT('LINESTRING(',
              latpoint-(r/units),' ',
              longpoint-(r /(units* COS(RADIANS(latpoint)))),
              ',', 
              latpoint+(r/units) ,' ',
              longpoint+(r /(units * COS(RADIANS(latpoint)))),
              ')')),  coordinates)
If you want to search for points within 20 km, change this line of the query

               20.0 AS r, 69.0 AS units
to this, for example

               20.0 AS r, 111.045 AS units
r is the radius in which you want to search.  units are the distance units (miles, km, furlongs, whatever you want) per degree of latitude on the surface of the earth.

This query uses a bounding lat/long along with MbrContains to exclude points that are definitely too far from your starting point, then uses the great circle distance formula to generate the distances for the remaining points. An explanation of all this can be found here. If your table uses the MyISAM access method and has a spatial index, MbrContains will exploit that index to get you fast searching.

Finally, the query above selects all the points within the rectangle. To narrow that down to only the points in the circle, and order them by proximity, wrap the query up like this:

 SELECT id, coordinates, name
   FROM (
         /* the query above, paste it in here */
        ) AS d
  WHERE d.distance <= d.r
  ORDER BY d.distance ASC 
"""