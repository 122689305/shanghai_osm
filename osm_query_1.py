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

node = 

database_close()