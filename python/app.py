# Cloud18 sample Python application: says hello and proves the database path end to end.
# The template passes the connection through the environment (DB_HOST is the cluster's
# proxy, so the answer comes from the current primary and follows a switchover).
import html
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pymysql

HOST = os.environ.get("DB_HOST", "127.0.0.1")
PORT = int(os.environ.get("DB_PORT", "3306"))
NAME = os.environ.get("DB_NAME", "")

PAGE = """<!DOCTYPE html>
<html>
  <head>
    <meta charset="utf-8">
    <title>Cloud18 sample Python application</title>
  </head>
  <body>
    <h1>{title}</h1>
    <p>{detail}</p>
  </body>
</html>
"""


class Hello(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            db = pymysql.connect(host=HOST, port=PORT, user=os.environ.get("DB_USER", ""),
                                 password=os.environ.get("DB_PASSWORD", ""), database=NAME, connect_timeout=3)
            with db, db.cursor() as cur:
                cur.execute("SELECT @@server_id, @@hostname, VERSION()")
                server_id, dbhost, version = cur.fetchone()
            status = 200
            title = "Hello world, I'm connected to database server id %d" % server_id
            detail = "Python, %s, %s, schema %s, through %s:%d" % (dbhost, version, NAME, HOST, PORT)
        except Exception as err:  # the page says why, the status says it failed
            status = 503
            title, detail = "Hello world, I'm not connected to the database", str(err)
        body = PAGE.format(title=html.escape(title), detail=html.escape(detail)).encode()
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", int(os.environ.get("PORT", "8000"))), Hello).serve_forever()
