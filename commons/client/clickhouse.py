from clickhouse_driver import dbapi

from config import ClickhouseConfig


class CHClient(object):
    def __init__(self, config: ClickhouseConfig):
        self.connection = dbapi.connect(database=config.database, user=config.user,
                                        password=config.password, host=config.host,
                                        port=config.port)

    def execute_and_fetchall(self, stmt) -> list:
        cursor = self.connection.cursor()
        cursor.execute(stmt)
        result = cursor.fetchall()
        cursor.close()
        return result
