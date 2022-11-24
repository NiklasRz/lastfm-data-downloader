import calendar
import datetime as dt
from queries import dbq as dbq
import pylast
import logging
import traceback

"""
A structured and systematic way of collecting data from lastfm

This is the base class.

Note: the data collection can be interrupted at any point. It will pick up where it left when you restart the process.

"""


class DataCollector(object):
    def __init__(self, network, config, pid):
        self.pid = pid
        self.logger = logging.getLogger("data_py_logger")
        self.config = config
        self.nw = network
        start = dt.datetime(
            self.config["timeframe"]["start_year"],
            self.config["timeframe"]["start_month"],
            1,
            0,
            0,
        )
        end = dt.datetime(
            self.config["timeframe"]["end_year"],
            self.config["timeframe"]["end_month"],
            1,
            0,
            0,
        )
        self.utc_start = calendar.timegm(start.utctimetuple())
        self.utc_end = calendar.timegm(end.utctimetuple())
        self.debug = self.config["debug"]

    def get_users(self):

        """
        Frist, we check how many done users we have.
        Then we fetch a user that is not done yet.
        We fetch their friends and add them.

        Data status:
          0 = friends are not fetched
          1 = friends are being fetched
          2 = friends have been fetched
          3 = listenings are being fetched
          4 = listenings have been fetched
          5 = broken user (might have deleted their account)
        """

        self.logger.info(f"p{self.pid}    Fetching users.")

        # count users that have been fully processed
        n_users = dbq.count_users_with_status_bigger(status=2)

        if self.config["limits"]["users"] and self.config["limits"]["users"] <= n_users:
            self.logger.info(
                f"p{self.pid}    Users processed: {n_users}. Reached limit. Continuing with listenings."
            )
            return "stop"

        self.logger.info(f"p{self.pid}    Users processed: {n_users}")

        # fetch users where we don't have the friendship data yet
        # we use relatively small batches because we run these in parallel
        users_to_fetch = dbq.get_users_with_no_data(n=1, status=0)

        # this is for the initialization if there are no users yet (we use seeds from reddit)
        if len(users_to_fetch) == 0 and n_users == 0:
            users_to_fetch = [{"id": 0, "name": x} for x in self.config["seeds"]]
            for u in users_to_fetch:
                us = self.nw.get_user(u["name"])
                dbq.add_user(user_object=us)
        elif len(users_to_fetch) == 0 and n_users != 0:
            self.logger.info(f"p{self.pid}    finished processing users.")
            return "stop"

        # mark these users as being fetched right now
        dbq.update_data_status(user_ids=[x["id"] for x in users_to_fetch], status=1)

        # fetch the friends and add them to the db
        for u in users_to_fetch:
            status = 2
            try:
                friends = self.nw.get_user(u["name"]).get_friends(limit=None)
            except pylast.PyLastError as E:
                if "Invalid API key" in str(E.__context__):
                    self.logger.error(f"p{self.pid}    Invalid API key")
                    return "stop"
                elif "no such page" in str(E.__context__):
                    friends = []
                elif "User not found" in str(E.__context__):
                    friends = []
                    status = 5
                    self.logger.info(f"p{self.pid}    Removed broken user.")
                else:
                    print("1: NEW ERROR", E.__class__.__name__, E.__context__)
            for fr in friends:
                friend_id = dbq.add_user(user_object=fr)
                if friend_id:  # sometimes a user can't be fetched
                    dbq.add_friendship(user1=u["id"], user2=friend_id)
            dbq.update_data_status(user_ids=[u["id"]], status=status)

        return "repeat"

    # @profile  # activate for speedtest
    def get_listens(self):

        """
        Frist, we check how many users we have where we already finished collecting their listening events.
        Then we fetch a user that are not done yet.
        We fetch their listens and add them.

        Data status:
         <2 = friends have not been fetched (not ready)
          2 = listenings have not been fetched
          3 = listenings are being fetched
          4 = listenings have been fetched
          5 = broken user (might have deleted their account)
        """

        self.logger.info(f"p{self.pid}    Fetching listens.")

        # count users that have been fully processed
        n_users = dbq.count_users_with_status(status=4)
        self.logger.info(f"p{self.pid}    User listenings processed: {n_users}")

        # fetch users where we don't have the listening data yet
        # we use relatively small batches because we run these in parallel
        users_to_fetch = dbq.get_users_with_no_data(n=1, status=2)

        # this is for when we are done
        if len(users_to_fetch) == 0 and n_users != 0:
            self.logger.info(f"p{self.pid}    Finished processing user listenings.")
            return "stop"

        # mark these users as being fetched right now
        dbq.update_data_status(user_ids=[x["id"] for x in users_to_fetch], status=3)

        # fetch the listenings and add them to the db
        for user in users_to_fetch:

            if self.debug:
                self.logger.debug(f"p{self.pid}    --user {user['id']}")

            user_obj = self.nw.get_user(user["name"])

            try:
                tracks = user_obj.get_recent_tracks(
                    time_from=self.utc_start,
                    time_to=self.utc_end,
                    limit=None,
                    stream=True,
                )
            except Exception:
                self.logger.info(f"p{self.pid}    User listening history is private.")
                dbq.update_data_is_private(user_id=user["id"])
                # sometimes we get an error "user must be logged in". Probably this means that the listening history of that user is private.
                tracks = []

            try:  # if we are using a stream of listenings, they are not evaluated immediately, hence, the error is thrown at this point. In the outer try loop we catch any weird and rare errors and retry to fetch the listenings
                try:  # In the inner try loop we catch the specific error where the user listening history requires a login (is private)
                    for v, track in enumerate(tracks):

                        if self.config["speedtest"]:
                            if v >= self.config["speedtest_sample"]:
                                print("Finished Speedtest")
                                return

                        # check if the artist is already in the db. if not, fetch it from the api and add it.
                        artist_name = track.track.artist.name
                        artist_id = dbq.get_artist_id_from_name(artist_name=artist_name)

                        if not artist_id:
                            try:
                                artist_mbid = track.track.artist.get_mbid()
                            except Exception:
                                # sometimes the artist can't be accessed. Don't know why, might have been removed from the data. This is NOT handling the case where an artist doesn't have a mbid, which happens much more frequently, but rather the case where an artist exists in the listens history but can't be found in the table of artists.
                                artist_mbid = None
                            artist_id = dbq.add_artist(
                                artist_name=artist_name, mb_id=artist_mbid
                            )

                        album_id = None
                        if self.config["include_albums"]:
                            # check if the album is already in the db. if not, fetch it from the api and add it.
                            try:
                                album = track.track.get_album()
                            except Exception:
                                # sometimes the album can't be accessed. Don't know why, might have been removed from the data. This is NOT handling the case where a song doesn't have an album, which happens much more frequently, but rather the case where an album exists in the listens history but can't be found in the table of albums.
                                album = None
                            if album:
                                album_id = dbq.get_album_id_from_name(
                                    album_name=album.title
                                )
                                if not album_id:
                                    album_mbid = album.get_mbid()
                                    album_id = dbq.add_album(
                                        album_name=album.title,
                                        mb_id=album_mbid,
                                        artist_id=artist_id,
                                    )

                        # check if the song is already in the db. if not, fetch and add to db.
                        song_name = track.track.title
                        song_id = dbq.get_song_id_from_name(
                            song_name=song_name, artist_id=artist_id
                        )
                        if not song_id:
                            try:
                                song_mbid = track.track.get_mbid()
                            except Exception:
                                # again - sometimes this fails as the song seems to be missing from the records. Not the case where a song doesn't have an mbid, but rather where the song is missing entirely.
                                song_mbid = None
                            song_id = dbq.add_song(
                                song_name=song_name,
                                song_mbid=song_mbid,
                                album_id=album_id,
                                artist_id=artist_id,
                            )

                        # add the listening event to the DB
                        dbq.add_listening(
                            user=user["id"], song=song_id, time=track.timestamp
                        )

                        if self.debug:
                            self.logger.debug(
                                f"p{self.pid} New song\n  user {user['name']} \n   artist {track.track.artist.name} \n   {track.track.__dict__}"
                            )
                except pylast.PyLastError as E:
                    # same as before but this is needed if we stream the events and fetch them from the generator rather than fetching them all at once
                    if "Login: User required to be logged in" in str(E.__context__):
                        self.logger.info(
                            f"p{self.pid}    User listening history is private."
                        )
                        dbq.update_data_is_private(user_id=user["id"])
            except Exception:
                self.logger.info(
                    f"p{self.pid}    Failed to fetch stream of listenings."
                )
                traceback.print_exc()
                return "repeat"

            dbq.update_data_status(user_ids=[user["id"]], status=4)

        self.logger.info(f"p{self.pid}    Finished batch.")
        return "repeat"

    def get_tags(self):

        """
        Fetch the tags of the artists.
        """

        self.logger.info(f"p{self.pid}    Fetching tags.")

        artists = dbq.get_artists_with_no_tags(n=100)

        if len(artists) == 0:
            self.logger.info(f"p{self.pid}    Finished fetching tags.")
            return "stop"

        for artist in artists:
            try:
                artist_obj = self.nw.get_artist(artist["name"])
                tags = artist_obj.get_top_tags(limit=5)
            except pylast.WSError as E:  # sometimes the artists are not in the DB.
                if "The artist you supplied could not be found" in str(E):
                    tags = []
                else:
                    self.logger.error(f"       Caught exception on fetching tags: {E},")
                    continue

            dbq.add_tags_to_artist(
                tags=[{"tag": x.item.name, "weight": x.weight} for x in tags],
                artist_id=artist["id"],
            )

        self.logger.info(f"p{self.pid}    Finished batch.")

        return "repeat"
