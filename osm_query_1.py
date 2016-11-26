import pymysql
import time

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
    begin_time = time.time()
    connection =  get_database_connection()
    cursor = connection.cursor()
    print('connected')

    # set default node_id as test
    node_id = 28111345
    # TODO: accept the query as longitude and latitude or as the id of the node

    cursor.execute('select WayId from WayNode where NodeId = %d'%node_id)
    for r in cursor.fetchall():
    	print 'way_id: %s'%r['WayId']

    cursor.execute('select count(WayId) as cnt from WayNode where NodeId = %d'%node_id)
    r = cursor.fetchone()
    print r['cnt']
    # TODO: do we need to attach the tag info to the way?

    print(time.strftime('%H:%M:%S', time.gmtime(time.time()-begin_time)))