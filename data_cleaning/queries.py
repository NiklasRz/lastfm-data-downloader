from db import DB
import random
import logging

logger = logging.getLogger("lfm_db_logger")

"""
These are the queries used to interact with the DB. All queries must be defined here and only here.

"""


class DataBaseQueries(object):
    def __init__(self):
        self.db = DB()

    def total_user_count(self):

        query = """
        SELECT
            COUNT(id_nb) AS cnt
        FROM users
        ;
        """

        self.db.cursor.execute(query)
        users = self.db.cursor.fetchone()["cnt"]
        return users

    def user_count_by_data_group(self):

        query = """
        SELECT
            dc.status AS status,
            COUNT(us.id_nb) AS cnt
        FROM users us
        INNER JOIN data_collection dc
        ON us.id_nb = dc.user
        GROUP BY dc.status
        ;
        """

        self.db.cursor.execute(query)
        users = {r["status"]: r["cnt"] for r in self.db.cursor.fetchall()}
        return users

    def total_song_count(self):

        query = """
        SELECT
            COUNT(id_nb) AS cnt
        FROM songs
        ;
        """

        self.db.cursor.execute(query)
        songs = self.db.cursor.fetchone()["cnt"]
        return songs

    def total_listen_count(self):

        query = """
        SELECT
            COUNT(id_nb) AS cnt
        FROM listens
        ;
        """

        self.db.cursor.execute(query)
        res = self.db.cursor.fetchone()["cnt"]
        return res

    def total_album_count(self):

        query = """
        SELECT
            COUNT(id_nb) AS cnt
        FROM albums
        ;
        """

        self.db.cursor.execute(query)
        res = self.db.cursor.fetchone()["cnt"]
        return res

    def total_artist_count(self):

        query = """
        SELECT
            COUNT(id_nb) AS cnt
        FROM artists
        ;
        """

        self.db.cursor.execute(query)
        res = self.db.cursor.fetchone()["cnt"]
        return res

    def estimate_release_date_from_listens(self):

        """
        This needs to run before we merge the songs, such that we find the song that was there first.
        """

        query = """
        SELECT
            song,
            MIN(time) AS release_date
        FROM listens l
        GROUP BY
            song
        ;
        """
        self.db.cursor.execute(query)
        params = [(x["release_date"], x["song"]) for x in self.db.cursor.fetchall()]

        query = """
        UPDATE
            songs
        SET
            released_lfm = ?
        WHERE
            id_nb = ?
        ;
        """
        self.db.cursor.executemany(query, params)
        self.db.connection.commit()

    def check_release_dates_added(self):

        query = """
        SELECT
            COUNT(*) AS cnt
        FROM songs
        WHERE released_lfm IS NULL
        ;
        """
        self.db.cursor.execute(query)
        res = self.db.cursor.fetchone()["cnt"]
        return res

    def get_listens_history_of_songs(self, n):

        query = """
        SELECT
            COUNT(*) AS cnt,
            song
        FROM listens
        GROUP BY song
        HAVING cnt > 100
        ;
        """
        self.db.cursor.execute(query)
        res = self.db.cursor.fetchall()
        songs = [x["song"] for x in res]
        random_songs = random.sample(songs, n)

        song_history = []

        for song in random_songs:
            query = f"""
            SELECT
                sub.monthyear AS t,
                COUNT(*) AS cnt,
                so.name AS song,
                a.name AS artist
            FROM (
                SELECT
                    *,
                    STRFTIME('%m %Y', DATETIME(time, 'unixepoch')) AS monthyear
                FROM listens
                WHERE song = {song}
                ORDER BY time
            ) sub
            INNER JOIN songs so
            ON so.id_nb = sub.song
            INNER JOIN artists a
            ON a.id_nb = so.artist
            GROUP BY monthyear
            ORDER BY time
            ;
            """
            self.db.cursor.execute(query)
            res = list(self.db.cursor.fetchall())
            data = {
                "x": [x["t"] for x in res],
                "y": [x["cnt"] for x in res],
                "t": f'{res[0]["song"]} | {res[0]["artist"]}',
            }
            song_history.append(data)

        return song_history

    def get_song_releases_per_month(self):

        query = """
        SELECT
            monthyear,
            COUNT(*) AS cnt
        FROM (
            SELECT
                *,
                MIN(time),
                STRFTIME('%m %Y', DATETIME(time, 'unixepoch')) AS monthyear
            FROM listens
            GROUP BY song
            ORDER BY time
          )
         GROUP BY monthyear
         ORDER BY time
         ;
        """
        self.db.cursor.execute(query)
        res = self.db.cursor.fetchall()
        return res

    def merge_songs(self):

        query = """
            INSERT OR IGNORE INTO song_merges(
                original_song,
                duplicate_song
            )
            SELECT
                newid AS original_id,
                oldid AS duplicate_id
            FROM (
                SELECT
                    sub2.id_nb AS newid,
                    sub1.id_nb AS oldid,
                    sub2.released
                FROM (
                    SELECT
                        id_nb,
                        name_full,
                        artist,
                        released,
                        artist_lower,
                        name
                    FROM (
                        SELECT
                            id_nb,
                            name_full,
                            artist,
                            released,
                            artist_lower,
                            name,
                            REPLACE(
                                CASE
                                    WHEN SUBSTR(name, 1, (INSTR(name, '-'))-1) ='' THEN name
                                    ELSE substr(name, 1, (INSTR(name, '-'))-1)
                                END,' ',''
                            ) AS name
                        FROM (
                            SELECT
                                s.id_nb,
                                s.name AS name_full,
                                a.name AS artist,
                                s.released_lfm AS released,
                                LOWER(a.name) AS artist_lower,
                                REPLACE(
                                    LOWER(
                                        CASE
                                            WHEN SUBSTR(s.name, 1, (INSTR(s.name, '('))-1) ='' THEN s.name
                                            ELSE substr(s.name, 1, (INSTR(s.name, '('))-1)
                                        END
                                    ),' ',''
                                ) AS name
                            FROM songs s
                            INNER JOIN
                                artists a
                            ON a.id_nb = s.artist
                        ) AS sub
                    ) AS sub
                ) sub1
                INNER JOIN (
                    SELECT
                        id_nb,
                        name_full,
                        artist,
                        released,
                        artist_lower,
                        name,
                        MIN(released)
                    FROM (
                        SELECT
                            id_nb,
                            name_full,
                            artist,
                            released,
                            artist_lower,
                            name
                        FROM (
                            SELECT
                                id_nb,
                                name_full,
                                artist,
                                released,
                                artist_lower,
                                name,
                                REPLACE(
                                    CASE
                                        WHEN SUBSTR(name, 1, (INSTR(name, '-'))-1) ='' THEN name
                                        ELSE SUBSTR(name, 1, (INSTR(name, '-'))-1)
                                    END,' ',''
                                ) AS name
                            FROM (
                                SELECT
                                    s.id_nb,
                                    s.name AS name_full,
                                    a.name AS artist,
                                    s.released_lfm AS released,
                                    LOWER(a.name) AS artist_lower,
                                    REPLACE(
                                        LOWER(
                                            CASE
                                                WHEN SUBSTR(s.name, 1, (INSTR(s.name, '('))-1) ='' THEN s.name
                                                ELSE SUBSTR(s.name, 1, (INSTR(s.name, '('))-1)
                                                END
                                            ),' ',''
                                    ) AS name
                                FROM songs s
                                INNER JOIN
                                    artists a
                                ON a.id_nb = s.artist
                            ) AS sub
                        ) AS sub
                    ) AS sub
                    GROUP BY artist_lower, name
                ) sub2
                ON sub1.artist_lower = sub2.artist_lower
                AND sub1.name = sub2.name
            ) sub3
            WHERE original_id != duplicate_id
            ;
        """

        self.db.cursor.execute(query)
        self.db.connection.commit()

    def get_duplicate_user_count(self):

        query = """
        SELECT
            (COUNT(name) - COUNT(DISTINCT name)) AS duplicates
        FROM users
        ;
        """

        self.db.cursor.execute(query)
        res = self.db.cursor.fetchone()["duplicates"]
        return res

    def insert_data(self):

        logger.info("    copying user data...")
        # copy the users
        query = """
        INSERT OR IGNORE INTO cleandb.users(
            id_nb,
            name,
            country,
            registered,
            total_listens,
            history_is_private,
            data_status
        )
        SELECT
            u.id_nb,
            u.name,
            u.country,
            u.registered,
            u.total_listens,
            u.history_is_private,
            dc.status
        FROM users u
        INNER JOIN
            data_collection dc
        ON dc.user = u.id_nb
        ;
        """
        self.db.cursor.execute(query)
        self.db.connection.commit()

        logger.info("    copying artist data...")
        query = """
        INSERT OR IGNORE INTO cleandb.artists(
            id_nb,
            name,
            mbid
        )
        SELECT
            id_nb,
            name,
            mb_id
        FROM artists
        ;
        """
        self.db.cursor.execute(query)
        self.db.connection.commit()

        logger.info("    copying album data...")
        query = """
        INSERT OR IGNORE INTO cleandb.albums(
            id_nb,
            name,
            mbid,
            artist,
            released
        )
        SELECT
            id_nb,
            name,
            mb_id,
            artist,
            released_lfm
        FROM albums
        ;
        """
        self.db.cursor.execute(query)
        self.db.connection.commit()

        logger.info("    copying friendship data...")
        query = """
        INSERT OR IGNORE INTO cleandb.friendships(
            user1,
            user2
        )
        SELECT
            user1,
            user2
        FROM friendships
        ;
        """
        self.db.cursor.execute(query)
        self.db.connection.commit()

        logger.info("    copying tags data...")
        query = """
        INSERT OR IGNORE INTO cleandb.tags(
            id_nb,
            tag,
            artist,
            weight
        )
        SELECT
            id_nb,
            tag,
            artist,
            weight
        FROM tags
        ;
        """
        self.db.cursor.execute(query)
        self.db.connection.commit()

        logger.info("    copying song data...")
        # copy the songs using merged song data. We don't copy any songs that are duplicates.
        query = """
        INSERT OR IGNORE INTO cleandb.songs(
            id_nb,
            mbid,
            name,
            artist,
            album,
            released
        )
        SELECT
            id_nb,
            mb_id,
            name,
            artist,
            album,
            released_lfm
        FROM songs
        WHERE id_nb NOT IN (
            SELECT
                duplicate_song
            FROM song_merges
        )
        ;
        """
        self.db.cursor.execute(query)
        self.db.connection.commit()

        logger.info("    copying listens data...")
        # copy the listens using merged song data. Listens to duplicate songs go to original songs instead
        query = """
        INSERT OR IGNORE INTO cleandb.listens(
            id_nb,
            user,
            song,
            time
        )
        SELECT
            l.id_nb,
            l.user,
            CASE WHEN sm.original_song IS NULL THEN l.song ELSE sm.original_song END AS true_id,
            l.time FROM listens l
        LEFT JOIN song_merges sm
        ON l.song = sm.duplicate_song
        ;
        """
        self.db.cursor.execute(query)
        self.db.connection.commit()

    def get_user_registrations_per_month(self):

        query = """
        SELECT
            monthyear,
            COUNT(*) AS cnt
        FROM (
            SELECT
                STRFTIME('%m %Y', DATETIME(registered, 'unixepoch')) AS monthyear,
                registered
            FROM users
          )
         GROUP BY monthyear
         ORDER BY registered
         ;
        """
        self.db.cursor.execute(query)
        res = self.db.cursor.fetchall()
        return res


dbq = DataBaseQueries()
