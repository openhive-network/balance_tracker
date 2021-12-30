import json
import psycopg2

class Db:
    def __init__(self, database, user, password, host, port):
        self.connection = psycopg2.connect(
            database=database, user=user, password=password, host=host, port=port
        )

    @staticmethod
    def parse_json(cursor):
        return json.dumps(cursor.fetchall()[0][0])

    def query(self, psql_cmd):
        cursor = self.connection.cursor()
        cursor.execute(psql_cmd)
        return self.parse_json(cursor)