import MySQLdb as mysql
import time

conn = None
cur = None

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

# set default node_id as test
node_id = 1
# TODO: accept the query as longitude and latitude or as the id of the node

cur.execute('select way_id from way_node where node_id = %s')
for r in cur.fetchall():
	print 'way_id: %s'%r.r['way_id']

cur.execute('select count(way_id) from way_node where node_id = %s')
# TODO: do we need to attach the tag info to the way?

database_close()