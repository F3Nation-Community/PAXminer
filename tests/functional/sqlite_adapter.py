"""
SQLiteAdapter is a wrapper class for SQLite connections that mimics the interface
of a MySQL connection as used in various scripts.

This adapter is necessary for testing purposes because:

1. It allows us to use SQLite (an in-memory database) for testing instead of MySQL,
   which doesn't require a separate database server.

2. It provides a compatible interface for the `with mydb.cursor() as cursor:` pattern
   used in the original scripts, which is not natively supported by SQLite.
"""
from contextlib import contextmanager


class SQLiteAdapter:
    def __init__(self, connection):
        self.connection = connection

    @contextmanager
    def cursor(self):
        cursor = self.connection.cursor()
        try:
            yield SQLiteCursorWrapper(cursor)
        finally:
            cursor.close()

    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()

    def close(self):
        self.connection.close()


class SQLiteCursorWrapper:
    def __init__(self, cursor):
        self.cursor = cursor

    def execute(self, query, params=None):
        # Convert MySQL paramstyle to SQLite
        query = query.replace("%s", "?")

        # Convert MySQL date functions to SQLite
        query = query.replace("MONTH(Date)", "CAST(strftime('%m', Date) AS INTEGER)")
        query = query.replace("YEAR(Date)", "CAST(strftime('%Y', Date) AS INTEGER)")

        print(f"Executing query: {query}")
        print(f"With parameters: {params}")

        if params is None:
            return self.cursor.execute(query)
        else:
            return self.cursor.execute(query, params)

    def fetchall(self):
        results = self.cursor.fetchall()
        print(f"Query returned {len(results)} rows")
        if results:
            print("First row of results:")
            print(results[0])
            print("Column names:")
            print([description[0] for description in self.cursor.description])
        return results

    def fetchone(self):
        result = self.cursor.fetchone()
        if result:
            print("Fetched one row:")
            print(result)
            print("Column names:")
            print([description[0] for description in self.cursor.description])
        return result
