import csv
import sys
# import nullmodel
import os
import sqlite3


class StreamDB(object):

    def __init__(self, db_file_path):
        self.connection = sqlite3.connect(db_file_path)
        self.cursor = self.connection.cursor()
        self.create_db()

    def create_db(self):
        print("Creating DB")

        q = "ATTACH database '../data/lastfmdb_stream.db' AS streamdb;"
        self.cursor.execute(q)

        q = """
        CREATE TABLE IF NOT EXISTS streamdb.nodes(
        id_nb INTEGER  NOT NULL PRIMARY KEY,
        realtime DATE NULL,
        intrinsictime INTEGER NULL,
        timebin INTEGER NOT NULL,
        name TEXT NULL
        );"""
        self.cursor.execute(q)
        self.connection.commit()

        q = """
        CREATE TABLE IF NOT EXISTS streamdb.stream(
        intrinsictime INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
        node INTEGER NOT NULL,
        node_origin INTEGER NULL,
        realtime DATE NULL,
        timebin INTEGER NOT NULL,
        FOREIGN KEY(node) REFERENCES nodes(id_nb),
        FOREIGN KEY(node_origin) REFERENCES nodes(id_nb)
        );"""
        self.cursor.execute(q)
        self.connection.commit()

        q = """
        INSERT INTO streamdb.nodes(id_nb, timebin)
        SELECT so.id_nb, (CAST (STRFTIME('%Y', DATETIME(lis.time, 'unixepoch')) AS INT) - 2005) * 12 + CAST(STRFTIME('%m', DATETIME(lis.time, 'unixepoch')) AS INT) AS 'timebin'
        FROM songs so
        INNER JOIN listens lis
        ON so.id_nb = lis.song
        WHERE lis.time BETWEEN 1104537600 AND 1388534400
        GROUP BY so.id_nb
        ORDER BY lis.time ASC
        """
        self.cursor.execute(q)
        self.connection.commit()

        q = """
        INSERT INTO streamdb.stream(node, realtime, timebin)
        SELECT song, time, (CAST (STRFTIME('%Y', DATETIME(time, 'unixepoch')) AS INT) - 2005) * 12 + CAST(STRFTIME('%m', DATETIME(time, 'unixepoch')) AS INT) AS 'timebin'
        FROM listens
        WHERE time BETWEEN 1104537600 AND 1388534400
        ORDER BY time ASC
        """
        self.cursor.execute(q)
        self.connection.commit()

        print("DB successfully created")


if __name__ == "__main__":
    cl = StreamDB(db_file_path="../data/lastfm_processed.db")
