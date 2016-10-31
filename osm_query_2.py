import MySQLdb as mysql
import time

def database_connect():
	global conn, cur
	conn = mysql.connect(host='localhost', user='root', passwd='0719', charset='utf8')
	cur = conn.cursor()
	cur.execute('set names utf8')

def database_init():
	conn.select_db('shanghai_osm')

def database_close():
	conn.commit()
	cur.close()
	conn.close()

database_connect()
database_init()

# set default way_id as test
way_id = 1
# TODO: accept the query as longitude and latitude or as the id of the way

cur.execute('select id, lat, lon from node RIGHT JOIN (\
	select node_id from way_node where way_id = %s) ON id = node_id', way_id)
print 'node_id\t\tlat\t\tlon'
for r in cur.fetchall():
	print '%s\t\t%d\t\t%d'%(r['id'],r['lat'],r['lon']

database_close()