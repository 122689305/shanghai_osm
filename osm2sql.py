
import xml.etree.ElementTree as ET
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

def database_create():
	cur.execute('drop database if exists shanghai_osm')
	cur.execute('create database shanghai_osm default character set utf8 collate utf8_general_ci')
	conn.select_db('shanghai_osm')
	cur.execute('create table node(\
					id varchar(20) not null primary key,\
					lat double not null,\
					lon double not null,\
					point point,\
					is_poi boolean default FALSE\
		)')
	cur.execute('create table way(\
					id varchar(20) not null primary key\
		)')
	cur.execute('create table node_tag(\
					node_id varchar(20) not null,\
					k	varchar(255) not null,\
					v	varchar(255) not null,\
					foreign key(node_id) references node(id)\
		)')
	cur.execute('create table way_tag(\
					way_id varchar(20) not null,\
					k	varchar(255) not null,\
					v	varchar(255) not null,\
					foreign key(way_id) references way(id)\
		)')
	cur.execute('create table way_node(\
					way_id	varchar(20) not null,\
					node_id	varchar(20) not null,\
					foreign key(way_id) references way(id),\
					foreign key(node_id) references node(id)\
		)')	

def database_close():
	cur.close()
	conn.close()

def database_node_insert():
	id = element.get('id')
	lon = element.get('lon')
	lat = element.get('lat')
	cur.execute('insert into node(id, lat, lon) values(%s, %s, %s)', (id, lat, lon))
	conn.commit()

def database_way_insert():
	id = element.get(id)
	cur.execute('insert into way(id) values(%s)', id)
	conn.commit()

def database_node_tag_insert():
	node_id = parent_element['id']
	k = element.get('k')
	v = element.get('v')
	cur.execute('insert into node_tag(node_id, k, v) values(%s, %s, %s)', (node_id, k, v))
	conn.commit()

def database_way_tag_insert():
	way_id = parent_element['id']
	k = element.get('k')
	v = element.get('v')
	cur.execute('insert into way_tag(way_id, k, v) values(%s, %s, %s)', (way_id, k, v))
	conn.commit()

def database_way_node_insert():
	way_id = parent_element['id']
	node_id = element.get('ref')
	cur.execute('insert into way_node(way_id, node_id) values(%s, %s)', (way_id, node_id))
	conn.commit()

database_connect()
database_create()
database_init()

begin_time = time.time()
tree = ET.iterparse('shanghai_dump.osm', events=("start", "end"))
tree = iter(tree)
event, root = tree.next()	# event = end
event, bounds = tree.next() # event = start
event, bounds = tree.next() # event = end

parent_element = {}
for event, element in tree:
	if event == 'start' and element.tag == 'node':
		parent_element['type'] = 'node'
		parent_element['id'] = element.get('id')
		database_node_insert()
		continue
	if event == 'start' and element.tag == 'way':
		parent_element['type'] = 'way'
		parent_element['id'] = element.get('id')
		database_way_insert()
		continue	
	if event == 'end' and element.tag == 'tag':
		if parent_element['type'] == 'node':
			database_node_tag_insert()
			continue
		if parent_element['type'] == 'way':
			database_way_tag_insert()
			continue
		continue
	if event == 'end' and element.tag == 'nd':
		database_way_node_insert()
		continue
	if element.tag == 'relation':
		break
end_time = time.time()
print 'elapsed: %f'%(begin_time-end_time)

database_close()