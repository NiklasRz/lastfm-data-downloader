# Last.fm data downloader

This is a simple Python program to fetch data from the last.fm API and store it in a SQL database. Data cleaning is included.

The output tables include:
- users (name, country, registration date, total listens)
- songs (name, artist, album, first time of appearance, musicbrainz ID if available)
- listens (time-ordered listening histories of the users)
- artists (name, musicbrainz ID if available)
- artist tags (including weights, these are the only tags that are frequently used)
- friendships (friendship links between users)

Data cleaning includes:
- check for duplicates
- check for weird timestamps
- merge duplicate songs of the same artist and the same songname (e.g. radio version and regular version)
- approximates the release date of a song by the first time it was listened to on last.fm (be careful, this is obviously wrong for songs that were released before last.fm existed)

## How to use

### Data Collection

A structured and systematic way of collecting data from lastfm

1. create last.fm API account
    https://www.last.fm/api/account/create

    This also works with multiple API keys but attention must be paid to the last.fm API terms of service.

2. save the credentials in config/last_fm_credentials.json. An example formatting can be found in ```config/last_fm_credentials_EXAMPLE.json```

3. configure ```config/config.yaml``` to your needs

4. from the base directory run: ```python3 data_collection/run.py```

    Depending on the number of API keys you are using and the amount of data you want to collect, this can take very long due to the last.fm rate limits at 1 request per key per second.

    You can check the progress at any time by running: ```python3 data_collection/quick_check.py```

    Note: the data collection can be interrupted at any point. It will pick up where it left when you restart the process.


The data_collection table keeps track of the data status of each fetched user. The following entries are possible:

- <2 = friends have not been fetched (not ready)
- 2 = friends have been fetched but listenings have not been fetched
- 3 = listenings are being fetched
- 4 = listenings have been fetched
- 5 = broken user (might have deleted their account)

A note on song release dates. Because the release dates of songs in both last.fm and musicbrainz are very unreliable, we approximate the release date as the date the has been listened to the first time on last.fm.


### Data Cleaning

1. from the base directory run ```python3 data_cleaning/run.py```  - this does not overwrite anything in the original DB, so all changes can be reverted.



### Data Stream (optional)

For certain analysis such as the analysis of attachment kernels, it is convenient to have the listening data formatted in a time-ordered stream of events where each song listening event is related to a song, a real time, an intrinsic time and a timebin. To create a separate database that contains this stream, run ```python3 data_stream/run.py```


## Publication

This data downloader was written as part of a scientific study. If you are using it for scientific purposes please consider citing [will be added soon]
