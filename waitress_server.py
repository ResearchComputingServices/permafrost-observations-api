#
# Production WSGI server
# @version 1.0
# @author Sergiu Buhatel <sergiu.buhatel@carleton.ca>
#

from waitress import serve
from app import app
serve(app, listen='192.168.2.12:7002', ipv4=True, ipv6=False, url_scheme='http')

