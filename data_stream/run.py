import sqlite3
from pathlib import Path
import yaml

"""
This is the class to create a stream of listening events used for specific types of analysis. Probably not relevant for most users.
"""


class StreamDB(object):
    def __init__(self, db_file_path):

        config_path = Path().absolute().joinpath("config").joinpath("config.yaml")
        with open(config_path, "r") as stream:
            self.config = yaml.safe_load(stream)

        self.connection = sqlite3.connect(db_file_path)
        self.cursor = self.connection.cursor()
        self.create_db()

    def create_db(self):
        print("Creating DB")

        stream_db_path = Path().absolute().joinpath("data").joinpath("lastfm_stream.db")
        q = f"ATTACH database '{stream_db_path}' AS streamdb;"
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

        q = f"""
        INSERT INTO streamdb.nodes(id_nb, timebin)
        SELECT so.id_nb, (CAST (STRFTIME('%Y', DATETIME(lis.time, 'unixepoch')) AS INT) - {self.config['timeframe']['start_year']}) * 12 + CAST(STRFTIME('%m', DATETIME(lis.time, 'unixepoch')) AS INT) - {self.config['timeframe']['start_month']} + 1 AS 'timebin'
        FROM songs so
        INNER JOIN listens lis
        ON so.id_nb = lis.song
        GROUP BY so.id_nb
        ORDER BY lis.time ASC
        """
        self.cursor.execute(q)
        self.connection.commit()

        q = f"""
        INSERT INTO streamdb.stream(node, realtime, timebin)
        SELECT song, time, (CAST (STRFTIME('%Y', DATETIME(time, 'unixepoch')) AS INT) - {self.config['timeframe']['start_year']}) * 12 + CAST(STRFTIME('%m', DATETIME(time, 'unixepoch')) AS INT) - {self.config['timeframe']['start_month']} + 1 AS 'timebin'
        FROM listens
        ORDER BY time ASC
        """
        self.cursor.execute(q)
        self.connection.commit()

        print("DB successfully created")


if __name__ == "__main__":
    db_file_path = Path().absolute().joinpath("data").joinpath("lastfm_processed.db")
    cl = StreamDB(db_file_path=db_file_path)
