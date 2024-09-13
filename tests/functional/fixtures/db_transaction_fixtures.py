import sqlite3
from time import time

import pytest


@pytest.fixture(scope='session')
def db_connection():
    conn = sqlite3.connect(':memory:')
    return conn


@pytest.fixture(scope='session')
def cursor(db_connection):
    return db_connection.cursor()


@pytest.fixture(scope='session', autouse=True)
def setup_database(db_connection, cursor):
    cursor.execute("PRAGMA foreign_keys = ON")

    start = time()
    print('Building database...')

    # This is where we could build basic DB tables needed by all tests. I'm leaving this blank for now.

    db_connection.commit()
    print('Done building database. {}'.format(time() - start))


@pytest.fixture(autouse=True)
def transaction(db_connection):
    db_connection.execute("BEGIN")
    yield
    db_connection.rollback()
