import configparser
import os

import pymysql


class DBConnectionManager:
    """
    Manages database connections using configuration from a file or environment variables.

    Use `connect()` to establish a connection or `connect(<db-name>)` to connect to a different database.
    Note: Connecting to a new database will close the previous connection.
    """

    def __init__(self, config_file: str = None):

        self.config = self._load_config(config_file)
        self._connection = None

    def _load_config(self, config_file: str) -> dict:

        if config_file:
            return self._load_from_file(config_file)
        else:
            return self._load_from_env()

    def _load_from_file(self, config_file: str) -> dict:

        config = configparser.ConfigParser()
        config.read(config_file)

        return {
            'host': config.get('DATABASE', 'host', fallback='localhost'),
            'port': config.getint('DATABASE', 'port', fallback=3306),
            'user': config.get('DATABASE', 'user'),
            'password': config.get('DATABASE', 'password'),
            'db': config.get('DATABASE', 'db'),
        }

    def _load_from_env(self) -> dict:
        return {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': int(os.getenv('DB_PORT', '3306')),
            'user': os.environ['DB_USER'],
            'password': os.environ['DB_PASSWORD'],
            'db': os.getenv('DB_NAME'),
        }

    def connect(self, db_name: str = None) -> pymysql.connections.Connection:
        """
        Establish a new database connection.

        :param db_name: Name of the database to connect to (optional)
        :return: Database connection object
        """
        if self._connection and self._connection.open:
            self._connection.close()

        connect_params = self.config.copy()
        if db_name:
            connect_params['db'] = db_name

        self._connection = pymysql.connect(
            **connect_params,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor,
        )

        return self._connection

    def __del__(self):
        self.close()

    def close(self):
        if self._connection and self._connection.open:
            self._connection.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
