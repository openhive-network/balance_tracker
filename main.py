#!/usr/bin/python3


from argparse import ArgumentParser
from server.serve import DBHandler

if __name__ == "__main__":
    argument_parser = ArgumentParser()
    argument_parser.add_argument('--host', type=str, help='TCP server host address. Default is "localhost"', default="localhost")
    argument_parser.add_argument('--port', type=int, help='Port for server listening. Default is "3000"', default=3000)
    args = argument_parser.parse_args()

    handler = partial(DBHandler)
    httpd = ForkHTTPServer((args.host, args.port), handler)
    print("serving at port %d" %args.port)
    httpd.serve_forever()