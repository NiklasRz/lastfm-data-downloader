from queries import dbq as dbq
import logging
from pathlib import Path


class LastFM(object):
    def __init__(self):

        # Logging
        logfile = Path().absolute().joinpath("data").joinpath("logfile_lfm_db.log")
        handler = logging.FileHandler(logfile)
        formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
        handler.setFormatter(formatter)
        self.logger = logging.getLogger("lfm_db_logger")
        self.logger.setLevel(logging.DEBUG)
        self.logger.addHandler(handler)  # to file
        self.logger.addHandler(logging.StreamHandler())  # to console

        # self.explore_example_song_histories()
        self.get_db_stats()
        self.estimate_song_release_dates()
        self.merge_songs()
        self.create_clean_db()

        dbq.db.connection.close()

    def get_db_stats(self):

        self.logger.info("\n\n======== Database Statistics =======\n\n")

        self.logger.info(f"\nTotal number of users: {dbq.total_user_count()}")

        user_counts = dbq.user_count_by_data_group()
        user_counts[0] = user_counts.get(0, 0) + user_counts.get(1, 0)
        user_counts[2] = user_counts.get(2, 0) + user_counts.get(3, 0)
        groups = {
            0: "no data",
            2: "friends available",
            4: "listenings + friends available",
            5: "deleted user",
        }
        self.logger.info(
            "\nUser data status:\n"
            + "".join(
                [f"    {groups[k]}: {user_counts.get(k, 0)}\n" for k in [0, 2, 4, 5]]
            )
        )

        self.logger.info(f"\nTotal number of songs: {dbq.total_song_count()}")
        self.logger.info(f"\nTotal number of listens: {dbq.total_listen_count()}")
        self.logger.info(f"\nTotal number of albums: {dbq.total_album_count()}")
        self.logger.info(f"\nTotal number of artists: {dbq.total_artist_count()}")

        self.logger.info(
            f"\nNumber of duplicate users: {dbq.get_duplicate_user_count()}"
        )

        self.logger.info(
            "\nNew songs per month:\n"
            + "".join(
                [
                    f"    {x['monthyear']}: {x['cnt']}\n"
                    for x in dbq.get_song_releases_per_month()
                ]
            )
        )

        self.logger.info(
            "\nNew users per month:\n"
            + "".join(
                [
                    f"    {x['monthyear']}: {x['cnt']}\n"
                    for x in dbq.get_user_registrations_per_month()
                ]
            )
        )

    def estimate_song_release_dates(self):

        """
        We estimate and insert the song release dates from the first time someone listend to that song. This is justified by explore_example_song_histories and get_song_releases_per_month.
        """

        self.logger.info(
            "\nEstimating song release dates from listenings (this could take a while)"
        )

        missing_release_dates = dbq.check_release_dates_added()
        if missing_release_dates == 0:
            overwrite = input("Existing release dates found. Overwrite? y/n\n")
            if overwrite == "y":
                dbq.estimate_release_date_from_listens()
            return
        dbq.estimate_release_date_from_listens()

    def merge_songs(self):

        self.logger.info("\nMerging songs (this could take a while)")

        dbq.merge_songs()

    def explore_example_song_histories(self):

        """
        In order to find an estimate for the release date of songs based on their listens, we explore the listens over time for some example songs here such that we can compare them to actual release dates found on google.
        """

        import matplotlib.pyplot as plt

        n_samples = 10
        histories = dbq.get_listens_history_of_songs(n=n_samples)

        for h in histories:
            plt.plot(h["x"], h["y"])
            plt.title(h["t"])
            plt.show()

    def create_clean_db(self):

        self.logger.info("\nCreating clean DB (this could take a while)")
        dbq.insert_data()
        self.logger.info("\nDone!")


if __name__ == "__main__":
    lfm = LastFM()
