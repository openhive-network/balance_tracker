from argparse import ArgumentParser
from server.serve import DBHandler, ForkHTTPServer
from functools import partial
import os

if __name__ == "__main__":
    argument_parser = ArgumentParser()
    argument_parser.add_argument('--host', type=str, help='TCP server host address. Default is "localhost"', default="localhost")
    argument_parser.add_argument('--port', type=int, help='Port for server listening. Default is "3000"', default=3000)
    argument_parser.add_argument('--log', type=int, help='Toggle request execution duration logging with any arg value (e.g. "yes", "1")', default=False)
    args = argument_parser.parse_args()    
    
    if args.log is not False:
        logs_dir = os.path.join(os.getcwd(), "logs")
        log_n = len(os.listdir(logs_dir))
        durations_path = os.path.join(logs_dir, "durations_log_%d.csv" %log_n)
        with open(durations_path, "w") as f:
            f.write("")

    handler = partial(DBHandler)
    httpd = ForkHTTPServer((args.host, args.port), handler)
    print("serving at port %d" %args.port)
    httpd.serve_forever()