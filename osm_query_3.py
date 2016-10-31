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

# set default way_name as test
way_name = 'xxx'
# TODO: accept the query as longitude and latitude and show a list of possible ways

cur.execute('select way_id from way_tag where k = "name" and v like "%s"', '%'+way_name)
# TODO: show more tags of the way_id returned

database_close()