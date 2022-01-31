from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse
import configparser
from server.adapter import Db
from db.backend import BalanceTracker
from socketserver import ForkingMixIn
from time import time
import os


class ForkHTTPServer(ForkingMixIn, HTTPServer):
    pass


class DBHandler(BaseHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        self.logging_setup()

        start = time()
        db, user, password, host, port = self.read_config()
        db = Db(db, user, password, host, port)
        self.balance_tracker = BalanceTracker(db)
        
        super().__init__(*args, **kwargs)
        self.log("connect", start)

    def logging_setup(self):
        logs_dir = os.path.join(os.getcwd(), "logs")
        log_ns = [int(name.strip(".csv").split("_")[-1]) for name in os.listdir(logs_dir)]
        if len(log_ns) > 0:
            log_n = max(log_ns)
        else:
            log_n = 0
        self.durations_path = os.path.join(logs_dir, "durations_log_%d.csv" %log_n)
        if os.path.isfile(self.durations_path):
            self.do_log = True
        else:
            self.do_log = False

    def log(self, name, start):
        if self.do_log:
            with open(self.durations_path, "a") as f:
                f.write("\n%s, %f" % (name, 1000 * (time() - start)))

    @staticmethod
    def read_config():
        config = configparser.ConfigParser()
        config.read("config.ini")
        config = config["POSTGRES"]
        return config["database"], config["user"], config["password"], config["host"], config["port"]

    def parse_params(self, required_params, param_types):
        now = time()
        params = dict(param.split("=")
                      for param in urlparse(self.path).query.split("&"))
        param_names = list(params.keys())
        for p, type in zip(required_params, param_types):
            if p not in param_names:
                raise Exception("Required parameter '%s' not provided" % p)
            else:
                params[p] = type(params[p])
        self.log("%s, parse_params" % self.path.split("/")[2], now)
        return params

    def send_reponse(self, http_code, content_type, response):
        now = time()
        self.send_response(http_code)
        self.send_header("Content-type", content_type)
        self.end_headers()
        self.wfile.write(str(response).encode())
        self.log("%s, send_reponse" % self.path.split("/")[2], now)

    def serve_favicon(self):
        now = time()
        self.send_response(200)
        self.send_header('Content-Type', 'image/x-icon')
        self.send_header('Content-Length', 0)
        self.end_headers()
        self.log("%s, serve_favicon" % self.path.split("/")[1], now)

    def do_GET(self):
        start = time()
        if self.path == "/":
            response = "Hello World"
            self.send_reponse(200, "application/json", response)

        elif self.path.startswith("/rpc/") and self.path.split("/")[2] == "find_matching_accounts":
            params = self.parse_params(["_partial_account_name"], [str])
            now = time()
            response = self.balance_tracker.find_matching_accounts(
                params["_partial_account_name"])
            self.log("%s, find_matching_accounts" %
                     self.path.split("/")[2], now)
            self.send_reponse(200, "application/json", response)

        elif self.path.startswith("/rpc/") and self.path.split("/")[2] == "get_balance_for_coin_by_block":
            params = self.parse_params(
                ["_account_name", "_coin_type", "_start_block", "_end_block"], [str, str, int, int])
            now = time()
            response = self.balance_tracker.get_balance_for_coin_by_block(
                params["_account_name"], params["_coin_type"], params["_start_block"], params["_end_block"])
            self.log("%s, get_balance_for_coin_by_block" %
                     self.path.split("/")[2], now)
            self.send_reponse(200, "application/json", response)

        elif self.path.startswith("/rpc/") and self.path.split("/")[2] == "get_balance_for_coin_by_time":
            params = self.parse_params(
                ["_account_name", "_coin_type", "_start_time", "_end_time"], [str, str, str, str])
            now = time()
            response = self.balance_tracker.get_balance_for_coin_by_time(
                params["_account_name"], params["_coin_type"], params["_start_time"], params["_end_time"])
            self.log("%s, get_balance_for_coin_by_time" %
                     self.path.split("/")[2], now)
            self.send_reponse(200, "application/json", response)

        elif self.path == "/favicon.ico":
            self.serve_favicon()

        else:
            response = "There's nothing here"
            self.send_reponse(404, "application/json", response)

        if self.path != "/favicon.ico":
            self.log("%s, total" % self.path.split("/")[2], start)
