import sqlite3
from pathlib import Path

"""
If you couldn't wait for the tags fo finish collecting before working on the DB (like me), this is the script used to retrospectively merge the tags table into the processed DB table.

Copy the DB file with the tags into the data folder and rename it to lastfm_tags.db
"""


class DB(object):
    def __init__(self):
        self.file_path_tags = (
            Path().absolute().joinpath("data").joinpath("lastfm_tags.db")
        )
        file_path_processed = (
            Path().absolute().joinpath("data").joinpath("lastfm_processed.db")
        )
        self.file_path = file_path_processed
        self.connect()

    def connect(self):
        self.connection = sqlite3.connect(self.file_path)
        self.connection.row_factory = (
            sqlite3.Row
        )  # DICT query results (misleading name imo)

        DB_INIT = f"""
        ATTACH database '{self.file_path_tags}' AS tagsdb;
        """

        self.cursor = self.connection.cursor()
        for cmd in DB_INIT.split(";"):
            self.cursor.execute(cmd)
            self.connection.commit()


class MergeTags(object):
    def __init__(self):
        self.db = DB()

    def merge(self):

        query = """
        INSERT OR IGNORE INTO tags (
            id_nb,
            tag,
            artist,
            weight
        )
        SELECT
            tt.id_nb,
            tt.tag,
            tt.artist,
            tt.weight
        FROM tagsdb.tags tt
        ;
        """
        self.db.cursor.execute(query)
        self.db.connection.commit()


MT = MergeTags()
MT.merge()
