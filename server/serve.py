from http.server import BaseHTTPRequestHandler
from urllib.parse import urlparse
import configparser
from server.adapter import Db
from db.backend import BalanceTracker

class DBHandler(BaseHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        db, user, password, host, port = self.read_config()
        db = Db(db, user, password, host, port)
        self.balance_tracker = BalanceTracker(db)
        super().__init__(*args, **kwargs)

    @staticmethod
    def read_config():
        config = configparser.ConfigParser()
        config.read("config.ini")
        config = config["POSTGRES"]
        return config["database"], config["user"], config["password"], config["host"], config["port"]

    def parse_params(self, required_params, param_types):
        params = dict(param.replace("%20", " ").split("=")
                      for param in urlparse(self.path).query.split("&"))
        param_names = list(params.keys())
        for p, type in zip(required_params, param_types):
            if p not in param_names:
                raise Exception("Required parameter '%s' not provided" % p)
            else:
                params[p] = type(params[p])
        return params

    def send_reponse(self, http_code, content_type, response):
        self.send_response(http_code)
        self.send_header("Content-type", content_type)
        self.end_headers()
        self.wfile.write(str(response).encode())

    def serve_favicon(self):
        self.send_response(200)
        self.send_header('Content-Type', 'image/x-icon')
        self.send_header('Content-Length', 0)
        self.end_headers()

    def do_GET(self):
        if self.path == "/":
            response = "Hello World"
            self.send_reponse(200, "application/json", response)

        elif self.path.startswith("/rpc/") and self.path.split("/")[2] == "find_matching_accounts":
            params = self.parse_params(["_partial_account_name"], [str])
            response = self.balance_tracker.find_matching_accounts(
                params["_partial_account_name"])
            self.send_reponse(200, "application/json", response)

        elif self.path.startswith("/rpc/") and self.path.split("/")[2] == "get_balance_for_coin_by_block":
            params = self.parse_params(
                ["_account_name", "_coin_type", "_start_block", "_end_block"], [str, str, int, int])
            response = self.balance_tracker.get_balance_for_coin_by_block(
                params["_account_name"], params["_coin_type"], params["_start_block"], params["_end_block"])
            self.send_reponse(200, "application/json", response)

        elif self.path.startswith("/rpc/") and self.path.split("/")[2] == "get_balance_for_coin_by_time":
            params = self.parse_params(
                ["_account_name", "_coin_type", "_start_time", "_end_time"], [str, str, str, str])
            response = self.balance_tracker.get_balance_for_coin_by_time(
                params["_account_name"], params["_coin_type"], params["_start_time"], params["_end_time"])
            self.send_reponse(200, "application/json", response)

        elif self.path == "/favicon.ico":
            self.serve_favicon()

        else:
            response = "There's nothing here"
            self.send_reponse(404, "application/json", response)