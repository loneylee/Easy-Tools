clickhouse_info = {
    'host': "localhost",
    'port': 9000,
    'database': "default",
    'user': 'default',
    'password': ''
}

output_dir="/home/admin123/Downloads"


class _ClickhouseConfig(object):
    @property
    def host(self):
        return clickhouse_info['host']

    @property
    def port(self):
        return clickhouse_info['port']

    @property
    def database(self):
        return clickhouse_info['database']

    @property
    def user(self):
        return clickhouse_info['user']

    @property
    def password(self):
        return clickhouse_info['password']


ClickhouseConfig = _ClickhouseConfig()
