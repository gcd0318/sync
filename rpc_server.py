from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
from node import Node
from common import md5
from config import rpc_port

class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)

server = SimpleXMLRPCServer(('0.0.0.0', rpc_port), requestHandler=RequestHandler)
server.register_introspection_functions()

server.register_function(md5)
server.register_instance(Node())

server.serve_forever()
