from api_pylast import DataCollector
import json
import pylast
from multiprocessing import Process
import time
import yaml
from queries import dbq as dbq
import logging
from pathlib import Path


class LastFM(object):

    def __init__(self):

        # Logging
        handler = logging.FileHandler('../data/logfile_lfm_collector.log')
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        handler.setFormatter(formatter)
        self.logger = logging.getLogger("data_py_logger")
        self.logger.setLevel(logging.DEBUG)
        self.logger.addHandler(handler)  # to file
        self.logger.addHandler(logging.StreamHandler())  # to console

        # load credentials
        if Path().absolute().joinpath("config").joinpath("last_fm_credentials.json").exists():
            credentials_path = Path().absolute().joinpath("config").joinpath("last_fm_credentials.json")
        else:
            credentials_path = Path().absolute().joinpath("config").joinpath("last_fm_credentials.json")
            with open(credentials_path, 'w'):
                pass
            self.logger.error("Please add your last.fm credentials to last_fm_credentials.json")
            return
        with open(credentials_path) as f:
            self.accounts = json.load(f)

        # load configs
        config_path = Path().absolute().joinpath("config").joinpath("config.yaml")
        with open(config_path, "r") as stream:
            self.config = yaml.safe_load(stream)

        self.get_data()

    def get_data(self):

        self.logger.info("\n\n======== NEW RUN =======\n\n")

        # reset the status of eventual entities that were marked as "being fetched" and cancelled before the fetching finished
        dbq.reset_cancelled_fetching()

        # for each API key, we start a process in parallel. Fetching data of one type must finish before the next step starts.
        if self.config["speedtest"]:
            credentials = self.accounts["1"]
            network = pylast.LastFMNetwork(
                api_key=credentials["key"],
                api_secret=credentials["secret"]
            )
            DataCollector(network=network, config=self.config, pid=0).get_listens()
            return

        for f in self.config["fetch"]:

            f = "get_" + f

            processes = []

            # first, test the credentials
            for a in self.accounts:
                credentials = self.accounts[a]
                network = pylast.LastFMNetwork(
                    api_key=credentials["key"],
                    api_secret=credentials["secret"]
                )
                self.test_credentials(network, credentials)

            for pid, a in enumerate(self.accounts):
                p = Process(target=self.runner, args=(self.accounts[a], pid, f, ))
                p.start()
                processes.append(p)
                time.sleep(self.config["sleep"]["between_process_inits"])

            for p in processes:
                p.join()

    def runner(self, credentials, pid, f):

        network = pylast.LastFMNetwork(
            api_key=credentials["key"],
            api_secret=credentials["secret"]
        )

        dc = DataCollector(network=network, config=self.config, pid=pid)

        f = getattr(dc, f)

        self.loop(f, pid)

    def loop(self, f, pid):

        i = 0
        timeout_counter = 0
        action = "repeat"
        while action == "repeat":

            i += 1

            try:
                action = f()
            except pylast.NetworkError as e:
                print("222", e.__class__.__name__, e.__class__.__qualname__, e.__context__)
                # 222 NetworkError NetworkError _ssl.c:1114: The handshake operation timed out
                self.logger.error(f" p{pid}    Caught exception: {e},")

                self.logger.info(f" p{pid}    sleeping {self.config['sleep']['sleep_short']}s")
                time.sleep(self.config["sleep"]["sleep_short"])

                # if we get timed out multiple times, wait 10 minutes before we try again. This is aimed at circumventing multiple possible reasons for exceptions e.g. internet problems, server downtime, server overload or rate limitations. The optimum sleep time must be found empirically.
                timeout_counter += 1
                if timeout_counter > self.config["sleep"]["n_timeouts_until_long_sleep"]:
                    self.logger.info(f" p{pid}    sleeping {self.config['sleep']['sleep_long']}s")
                    time.sleep(self.config["sleep"]["sleep_long"])
                    timeout_counter = 0

    def test_credentials(self, network, credentials):

        try:
            my_user = network.get_user(credentials["username"])
            my_user.get_registered()
        except Exception:
            raise Exception(f"Invalid user credentials on user {credentials['username']}")


if __name__ == "__main__":
    lfm = LastFM()
