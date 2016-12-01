from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
import urllib2
import os
import queries
import json
from urlparse import urlparse, parse_qs
import traceback
import sys


class S(BaseHTTPRequestHandler):
    def _set_headers(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()

    def do_GET(self):
        try:
            parsed_url = urlparse(self.path)
            path = parsed_url.path
            if path == '/':
                self.wfile.write(open('templates/index.html').read())
            else:
                parameters = parse_qs(parsed_url.query)
                if path == '/1':
                    self.wfile.write(queries.query1(int(parameters['id'][0])))
                elif path == '/2':
                    self.wfile.write(queries.query2(int(parameters['id'][0])))
                elif path == '/3':
                    self.wfile.write(queries.query3(parameters['name'][0]))
                elif path == '/4':
                    self.wfile.write(queries.query4(float(parameters['lat'][0]), float(parameters['lon'][0]), float(parameters['r'][0])))
                elif path == '/5':
                    self.wfile.write(queries.query5(float(parameters['lat'][0]), float(parameters['lon'][0])))
                elif path == '/6':
                    # self.wfile.write("zhangpeishiyoududi")
                    self.wfile.write(queries.query6(float(parameters['lat0'][0]), float(parameters['lon0'][0]), float(parameters['lat1'][0]), float(parameters['lon1'][0])))
                else:
                    self.wfile.write('error')
        except:
            print(parameters)
            traceback.print_exc(file=sys.stdout)
            self.wfile.write('error')

    def do_HEAD(self):
        self._set_headers()

    def do_POST(self):
        # Doesn't do anything with posted data
        try:
            self.wfile.write('no post')
        except Exception as e:
            traceback.print_exc(file=sys.stdout)
            print(str(e))
            self.wfile.write('Error: ' + str(e))


def run(server_class=HTTPServer, handler_class=S, port=10080):
    server_address = ('', port)
    httpd = server_class(server_address, handler_class)
    print('Starting httpd...')
    httpd.serve_forever()


if __name__ == '__main__':
    queries.init()
    run()
