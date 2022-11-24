import pylast
from db import DB
import sqlite3
from time import sleep
from time import time as timer
import logging
import yaml

logger = logging.getLogger("data_py_logger")

"""
These are the queries used to interact with the DB. All queries must be defined here and only here.

USE PARAMETERIZED QUERIES
Be aware, that despite this being a data science project without users, we need to protect our DB from SQL injection since theoretically there could be malicious user names (artist names, album names, tags, ...) in the last.fm data set that e.g. drop our tables.

"""


def debug_timer(func):

    def inner1(*args, **kwargs):

        if not args[0].config["debug"]:
            return func(*args, **kwargs)

        start = timer()
        res = func(*args, **kwargs)
        end = timer()
        logger.info(f"           ----t: {func.__name__} {end - start}")
        return res

    return inner1


def retry_if_locked(func):

    def inner2(*args, **kwargs):

        while True:
            try:
                return func(*args, **kwargs)
            except sqlite3.OperationalError:
                logger.info("           DB locked, waiting 3 seconds.")
                sleep(3)

    return inner2


class DataBaseQueries(object):

    def __init__(self):
        self.db = DB()
        self.logger = logging.getLogger("data_py_logger")

        with open("config.yaml", "r") as stream:
            self.config = yaml.safe_load(stream)

    @retry_if_locked
    @debug_timer
    def get_users_with_no_data(self, n, status):

        query = f"""
        SELECT
            us.id_nb,
            us.name,
            us.total_listens
        FROM users us
        INNER JOIN data_collection fd
        ON fd.user = us.id_nb
        WHERE fd.status = {status}
        LIMIT {n}
        ;
        """

        self.db.cursor.execute(query)
        users = list(self.db.cursor.fetchall())
        users = [{"id": x["id_nb"], "name": x["name"], "listens": x["total_listens"]} for x in users]
        return users

    @retry_if_locked
    @debug_timer
    def count_users_with_status(self, status):

        query = f"""
        SELECT
            COUNT(us.name)
        FROM users us
        INNER JOIN data_collection fd
        ON fd.user = us.id_nb
        WHERE fd.status = {status}
        ;
        """

        self.db.cursor.execute(query)
        return self.db.cursor.fetchone()[0]

    @retry_if_locked
    @debug_timer
    def update_data_status(self, status, user_ids=None, listens=None, songs=None):

        if user_ids:

            query = f"""
            UPDATE data_collection
            SET status = ?
            WHERE user IN ({", ".join([str(x) for x in user_ids])});
            """

            params = [status]
            self.db.cursor.execute(query, params)
            self.db.connection.commit()

    @retry_if_locked
    @debug_timer
    def add_user(self, user_object):

        query = """
        SELECT
            id_nb
        FROM users
        WHERE name = ?
        ;
        """
        params = [str(user_object)]
        self.db.cursor.execute(query, params)
        user_id = self.db.cursor.fetchone()
        exists = (True if user_id else False)

        if exists:
            return user_id[0]

        user_name = str(user_object)
        try:
            country = user_object.get_country()
        except pylast.WSError as E:
            if "Connection to the API failed" in str(E):
                return self.add_user(user_object)
            else:
                # User is not found error. It sometimes seems to happen that a user object is found in the friends list but data about it can't be retrieved.
                return None

        try:
            total_listens = user_object.get_playcount()
        except pylast.WSError as E:
            if "Connection to the API failed" in str(E):
                return self.add_user(user_object)
            else:
                # User is not found error.
                return None

        if country:
            country = country.name
        else:
            country = None
        try:
            registered = user_object.get_registered()
        except pylast.WSError as E:
            if "Connection to the API failed" in str(E):
                return self.add_user(user_object)
            else:
                # User is not found error.
                return None

        query = """
        INSERT INTO users
            (name, country, registered, total_listens)
        VALUES (?, ?, ?, ?)
        ON CONFLICT (name)
        DO NOTHING;
        """
        params = (user_name, country, registered, total_listens)
        self.db.cursor.execute(query, params)
        user_id = self.db.cursor.lastrowid
        self.db.connection.commit()

        query = """
        INSERT INTO data_collection
            (user, status)
        VALUES (?, ?)
        ON CONFLICT (user)
        DO NOTHING;
        """
        params = (user_id, 0)
        self.db.cursor.execute(query, params)
        self.db.connection.commit()

        return user_id

    @retry_if_locked
    @debug_timer
    def add_friendship(self, user1, user2):

        u1, u2 = sorted([user1, user2])

        query = """
        INSERT INTO friendships
            (user1, user2)
        VALUES (?, ?)
        ON CONFLICT (user1, user2)
        DO NOTHING;
        """
        params = (u1, u2)
        self.db.cursor.execute(query, params)
        self.db.connection.commit()

    @retry_if_locked
    @debug_timer
    def get_album_id_from_name(self, album_name):

        query = """
        SELECT
            id_nb
        FROM albums
        where name = ?
        ;
        """
        params = [album_name]
        self.db.cursor.execute(query, params)
        res = self.db.cursor.fetchone()
        return res["id_nb"] if res else None

    @retry_if_locked
    @debug_timer
    def add_album(self, album_name, mb_id, artist_id):

        query = """
        INSERT INTO albums (
            name,
            mb_id,
            artist
        )
        VALUES (?, ?, ?)
        ON CONFLICT (name)
        DO NOTHING;
        """
        params = (album_name, mb_id, artist_id)
        self.db.cursor.execute(query, params)
        self.db.connection.commit()
        return self.db.cursor.lastrowid

    @retry_if_locked
    @debug_timer
    def get_artist_id_from_name(self, artist_name):

        query = """
        SELECT
            id_nb
        FROM artists
        where name = ?
        ;
        """

        params = [artist_name]
        self.db.cursor.execute(query, params)
        res = self.db.cursor.fetchone()
        return res["id_nb"] if res else None

    @retry_if_locked
    @debug_timer
    def add_artist(self, artist_name, mb_id):

        query = """
        INSERT INTO artists (
            name,
            mb_id
        )
        VALUES (?, ?)
        ON CONFLICT (name)
        DO NOTHING;
        """
        params = (artist_name, mb_id)
        self.db.cursor.execute(query, params)
        self.db.connection.commit()
        return self.db.cursor.lastrowid

    @retry_if_locked
    @debug_timer
    def get_song_id_from_name(self, song_name, artist_id):

        query = """
        SELECT
            id_nb
        FROM songs
        where name = ?
        AND artist = ?
        ;
        """
        params = [song_name, artist_id]
        self.db.cursor.execute(query, params)
        res = self.db.cursor.fetchone()
        return res["id_nb"] if res else None

    @retry_if_locked
    @debug_timer
    def add_song(self, song_name, song_mbid, album_id, artist_id):

        """
        we accept here that some songs might be duplicates. we rather add the duplicates and later on merge them than omit them.
        """

        query = """
        INSERT INTO songs (
            name,
            mb_id,
            artist,
            album
        )
        VALUES (?, ?, ?, ?)
        ;
        """
        params = (song_name, song_mbid, artist_id, album_id)
        self.db.cursor.execute(query, params)
        self.db.connection.commit()
        return self.db.cursor.lastrowid

    @retry_if_locked
    @debug_timer
    def add_listening(self, user, song, time):

        query = """
        INSERT INTO listens (
            user,
            song,
            time
        )
        VALUES (?, ?, ?)
        ON CONFLICT (user, song, time)
        DO NOTHING;
        """
        params = (user, song, time)
        self.db.cursor.execute(query, params)
        self.db.connection.commit()

    @retry_if_locked
    @debug_timer
    def get_artists_with_no_tags(self, n):

        query = f"""
        SELECT
            a.id_nb,
            a.name
        FROM artists a
        LEFT JOIN tags t
        ON t.artist = a.id_nb
        WHERE t.tag IS NULL
        ORDER BY a.id_nb
        LIMIT {n}
        ;
        """

        self.db.cursor.execute(query)
        users = list(self.db.cursor.fetchall())
        users = [{"id": x["id_nb"], "name": x["name"]} for x in users]
        return users

    @retry_if_locked
    @debug_timer
    def add_tags_to_artist(self, tags, artist_id):

        if len(tags) == 0:
            tags = [{"tag": "NONE", "weight": 0}]

        for t in tags:
            query = """
            INSERT INTO tags (
                tag,
                artist,
                weight
            )
            VALUES (?, ?, ?)
            ON CONFLICT (artist, tag)
            DO NOTHING;
            """
            params = (t["tag"], artist_id, t["weight"])
            self.db.cursor.execute(query, params)
            self.db.connection.commit()

    @retry_if_locked
    @debug_timer
    def update_data_is_private(self, user_id):

        query = """
        UPDATE users
        SET history_is_private = TRUE
        WHERE id_nb = ?;
        """

        params = [user_id]
        self.db.cursor.execute(query, params)
        self.db.connection.commit()

    @retry_if_locked
    @debug_timer
    def reset_cancelled_fetching(self):

        query = """
        UPDATE data_collection
        SET status = 0
        WHERE status = 1
        ;
        """
        self.db.cursor.execute(query)

        query = """
        UPDATE data_collection
        SET status = 2
        WHERE status = 3
        ;
        """
        self.db.cursor.execute(query)

        self.db.connection.commit()

    def get_data_stats(self):

        query = """
        SELECT
            COUNT(*) AS cnt
        FROM songs
        ;
        """
        self.db.cursor.execute(query)
        songs = self.db.cursor.fetchone()["cnt"]

        query = """
        SELECT
            COUNT(*) AS cnt
        FROM listens
        ;
        """
        self.db.cursor.execute(query)
        listens = self.db.cursor.fetchone()["cnt"]

        query = """
        SELECT
            COUNT(*) AS cnt
        FROM artists
        ;
        """
        self.db.cursor.execute(query)
        artists = self.db.cursor.fetchone()["cnt"]

        query = """
        SELECT
            COUNT(*) AS cnt
        FROM tags
        ;
        """
        self.db.cursor.execute(query)
        tags = self.db.cursor.fetchone()["cnt"]

        query = """
        SELECT
            COUNT(DISTINCT a.id_nb) AS cnt
        FROM artists a
        INNER JOIN tags t
        ON t.artist = a.id_nb
        ;
        """
        self.db.cursor.execute(query)
        missing_tags = self.db.cursor.fetchone()["cnt"]

        return songs, listens, artists, tags, missing_tags

    @retry_if_locked
    @debug_timer
    def count_artists_with_no_tags(self):

        query = """
        SELECT
            COUNT(a.*)
        FROM artists a
        LEFT JOIN tags t
        ON t.artist = a.id_nb
        WHERE t.tag IS NULL
        ;
        """

        self.db.cursor.execute(query)
        return self.db.cursor.fetchone()[0]


dbq = DataBaseQueries()
