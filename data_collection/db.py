import sqlite3  # is included in core
from pathlib import Path
import os

"""
Here we define and initialize the database tables for the data collector.

Remember that song names are not unique. Hence also no unique indexing.

"""

file_path = Path().absolute().joinpath("data").joinpath("lastfm_raw.db")
if not file_path.exists():
    os.mkdir(Path().absolute().joinpath("data"))


DB_INIT = """

CREATE TABLE IF NOT EXISTS users(
    id_nb INTEGER  NOT NULL PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL,
    country TEXT NULL,
    registered DATE NOT NULL,
    total_listens INTEGER NULL,
    history_is_private BOOLEAN DEFAULT FALSE
);

CREATE UNIQUE INDEX IF NOT EXISTS index_user_name
ON users(name);

CREATE TABLE IF NOT EXISTS songs(
    id_nb INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    mb_id TEXT NULL,
    name TEXT NOT NULL,
    artist INT NOT NULL,
    album INT NULL,
    released_mb DATE NULL,
    released_lfm DATE NULL,
    FOREIGN KEY(artist) REFERENCES artists(id_nb)
);

CREATE UNIQUE INDEX IF NOT EXISTS index_song_artist ON songs(name, artist);

CREATE TABLE IF NOT EXISTS listens(
    id_nb INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    user INTEGER NOT NULL,
    song TEXT NOT NULL,
    time DATE NOT NULL,
    FOREIGN KEY(user) REFERENCES users(id_nb),
    FOREIGN KEY(song) REFERENCES songs(id_nb),
    CONSTRAINT UC_listens UNIQUE (user, song, time)
);

CREATE TABLE IF NOT EXISTS artists(
    id_nb INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    mb_id TEXT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS index_artist_name
ON artists(name);

CREATE TABLE IF NOT EXISTS albums(
    id_nb INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    mb_id TEXT NULL,
    artist INT NOT NULL,
    released_mb DATE NULL,
    released_lfm DATE NULL,
    FOREIGN KEY(artist) REFERENCES artists(id_nb)
);

CREATE UNIQUE INDEX IF NOT EXISTS index_album_name
ON albums(name);

CREATE TABLE IF NOT EXISTS tags(
    id_nb INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    tag TEXT NOT NULL,
    artist INT NOT NULL,
    weight INTEGER NOT NULL,
    FOREIGN KEY(artist) REFERENCES artists(id_nb),
    CONSTRAINT UC_tag UNIQUE (tag, artist)
);

CREATE TABLE IF NOT EXISTS friendships(
    user1 INTEGER NOT NULL,
    user2 INTEGER NOT NULL,
    FOREIGN KEY(user1) REFERENCES users(id_nb),
    FOREIGN KEY(user2) REFERENCES users(id_nb),
    CONSTRAINT UC_fs UNIQUE (user1, user2)
);

CREATE TABLE IF NOT EXISTS song_merges(
    original_song INTEGER NOT NULL,
    duplicate_song INTEGER NOT NULL,
    FOREIGN KEY(original_song) REFERENCES songs(id_nb),
    FOREIGN KEY(duplicate_song) REFERENCES songs(id_nb),
    CONSTRAINT UC_songmerge UNIQUE (original_song, duplicate_song)
);

CREATE TABLE IF NOT EXISTS data_collection(
    user INTEGER UNIQUE NOT NULL PRIMARY KEY,
    status INTEGER DEFAULT 0,
    FOREIGN KEY(user) REFERENCES users(id_nb)
);

"""


class DB(object):

    def __init__(self):
        self.file_path = file_path
        self.connect()

    def connect(self):
        self.connection = sqlite3.connect(self.file_path)
        self.connection.row_factory = sqlite3.Row  # DICT query results (Hey what's a good name for this method that returns dictionaries instead of rows? IDK, let's call it row_factory!)
        self.cursor = self.connection.cursor()
        for cmd in DB_INIT.split(';'):
            self.cursor.execute(cmd)
            self.connection.commit()
