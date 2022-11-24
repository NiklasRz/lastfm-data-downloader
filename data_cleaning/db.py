import sqlite3  # is included in core
from pathlib import Path

"""
Here we define and initialize the database tables for the final last.fm data base that we will use for analysis.

Notes:
- song name is not unique, even after the merge because obviously multiple songs of different artists can have the same name

"""

file_path_raw = Path().absolute().joinpath("data").joinpath("lastfm_raw.db")
file_path_processed = Path().absolute().joinpath("data").joinpath("lastfm_processed.db")

DB_INIT = f"""

ATTACH database '{file_path_processed}' AS cleandb;

CREATE TABLE IF NOT EXISTS cleandb.users(
    id_nb INTEGER  NOT NULL PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL,
    country TEXT NULL,
    registered DATE NOT NULL,
    total_listens INTEGER NULL,
    history_is_private BOOLEAN DEFAULT FALSE,
    data_status INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS cleandb.songs(
    id_nb INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    mbid TEXT NULL,
    name TEXT NOT NULL,
    artist INT NOT NULL,
    album INT NULL,
    released DATE NULL,
    FOREIGN KEY(artist) REFERENCES artists(id_nb),
    FOREIGN KEY(album) REFERENCES albums(id_nb)
);

CREATE TABLE IF NOT EXISTS cleandb.listens(
    id_nb INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    user INTEGER NOT NULL,
    song INTEGER NOT NULL,
    time DATE NOT NULL,
    FOREIGN KEY(user) REFERENCES users(id_nb),
    FOREIGN KEY(song) REFERENCES songs(id_nb),
    CONSTRAINT UC_listens UNIQUE (user, song, time)
);

CREATE TABLE IF NOT EXISTS cleandb.tags(
    id_nb INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    tag TEXT NOT NULL,
    artist INT NOT NULL,
    weight INTEGER NOT NULL,
    FOREIGN KEY(artist) REFERENCES artists(id_nb),
    CONSTRAINT UC_tag UNIQUE (tag, artist)
);

CREATE TABLE IF NOT EXISTS cleandb.friendships(
    user1 INTEGER NOT NULL,
    user2 INTEGER NOT NULL,
    FOREIGN KEY(user1) REFERENCES users(id_nb),
    FOREIGN KEY(user2) REFERENCES users(id_nb),
    CONSTRAINT UC_fs UNIQUE (user1, user2)
);

CREATE TABLE IF NOT EXISTS cleandb.artists(
    id_nb INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    mbid TEXT NULL
);

CREATE TABLE IF NOT EXISTS cleandb.albums(
    id_nb INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    mbid TEXT NULL,
    artist INT NOT NULL,
    released DATE NULL,
    FOREIGN KEY(artist) REFERENCES artists(id_nb)
);

"""


class DB(object):
    def __init__(self):
        self.file_path = file_path_raw
        self.connect()

    def connect(self):
        self.connection = sqlite3.connect(self.file_path)
        self.connection.row_factory = sqlite3.Row  # DICT query results
        self.cursor = self.connection.cursor()
        for cmd in DB_INIT.split(";"):
            self.cursor.execute(cmd)
            self.connection.commit()
