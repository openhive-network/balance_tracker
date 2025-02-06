from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs
import configparser
from adapter import Db
import sys
import os
import traceback
from socketserver import ForkingMixIn

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db.backend import BalanceTracker

class ForkHTTPServer(ForkingMixIn, HTTPServer):
    pass

class DBHandler(BaseHTTPRequestHandler):
    balance_tracker = None

    @staticmethod
    def read_config():
        config = configparser.ConfigParser()
        config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'py_config.ini')
        if not config.read(config_path):
            raise Exception(f"Config file not found at {config_path}")
        config = config["POSTGRES"]
        return config["database"], config["user"], config["password"], config["host"], config["port"]

    def parse_params(self, required_params, param_types):
        parsed_path = urlparse(self.path)
        params = parse_qs(parsed_path.query)
        
        parsed_params = {}
        for p, type in zip(required_params, param_types):
            if p not in params:
                raise Exception(f"Required parameter '{p}' not provided")
            else:
                parsed_params[p] = type(params[p][0])
        return parsed_params

    def send_reponse(self, http_code, content_type, response):
        try:
            self.send_response(http_code)
            self.send_header("Content-type", content_type)
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(str(response).encode())
        except Exception as e:
            print(f"Error sending response: {e}")

    def serve_favicon(self):
        self.send_response(200)
        self.send_header('Content-Type', 'image/x-icon')
        self.send_header('Content-Length', 0)
        self.end_headers()

    def do_GET(self):
        try:
            print(f"Received request: {self.path}")
            parsed_path = urlparse(self.path)
            path_parts = parsed_path.path.split('/')

            if parsed_path.path == "/":
                response = "Balance Tracker API"
                self.send_reponse(200, "application/json", response)

            elif len(path_parts) >= 3 and path_parts[1] == "rpc":
                if path_parts[2] == "find_matching_accounts":
                    params = parse_qs(parsed_path.query)
                    _partial_account_name = params.get('_partial_account_name', [''])[0]
                    print(f"Finding accounts matching: {_partial_account_name}")
                    response = self.balance_tracker.find_matching_accounts(_partial_account_name)
                    print(f"Response: {response}")
                    self.send_reponse(200, "application/json", response)

                elif path_parts[2] == "get_balance_for_coin_by_block":
                    params = self.parse_params(
                        ["_account_name", "_coin_type", "_start_block", "_end_block"], 
                        [str, str, int, int]
                    )
                    response = self.balance_tracker.get_balance_for_coin_by_block(
                        params["_account_name"], params["_coin_type"], 
                        params["_start_block"], params["_end_block"]
                    )
                    self.send_reponse(200, "application/json", response)

                elif path_parts[2] == "get_balance_for_coin_by_time":
                    params = self.parse_params(
                        ["_account_name", "_coin_type", "_start_time", "_end_time"], 
                        [str, str, str, str]
                    )
                    response = self.balance_tracker.get_balance_for_coin_by_time(
                        params["_account_name"], params["_coin_type"], 
                        params["_start_time"], params["_end_time"]
                    )
                    self.send_reponse(200, "application/json", response)

                elif path_parts[2] == "get_account_special_balances":
                    params = self.parse_params(["_account_name"], [str])
                    response = self.balance_tracker.get_account_special_balances(
                        params["_account_name"]
                    )
                    self.send_reponse(200, "application/json", response)

                else:
                    response = "Invalid RPC method"
                    self.send_reponse(404, "application/json", response)

            elif parsed_path.path == "/favicon.ico":
                self.serve_favicon()

            else:
                response = "Endpoint not found"
                self.send_reponse(404, "application/json", response)

        except Exception as e:
            print(f"Error handling request: {e}")
            print(f"Full traceback: {traceback.format_exc()}")
            self.send_reponse(500, "application/json", f"Server error: {str(e)}")

def run(server_class=ForkHTTPServer, handler_class=DBHandler):
    try:
        db, user, password, host, port = DBHandler.read_config()
        db = Db(db, user, password, host, port)
        DBHandler.balance_tracker = BalanceTracker(db)
        
        server_address = ('', 3000)
        httpd = server_class(server_address, handler_class)
        print('Starting server on port 3000...')
        httpd.serve_forever()
    except Exception as e:
        print(f"Error starting server: {e}")
        print(f"Full traceback: {traceback.format_exc()}")

if __name__ == '__main__':
    run()