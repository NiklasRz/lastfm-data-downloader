# Last.fm data downloader

This is a simple Python program to fetch data from the last.fm API and store it in a SQL database. Data cleaning is included.

The output tables include:
- users (name, country, registration date, total listens)
- songs (name, artist, album, first time of appearance)
- listens (time-ordered listening histories of the users)
- artist tags (including weights, these are the only tags that are frequently used)
- friendships (friendship links between users)


## How to use

### Data Collection

A structured and systematic way of collecting data from lastfm

1. create last.fm API account
    https://www.last.fm/api/account/create

    This also works with multiple API keys but attention must be paid to the last.fm API terms of service.

2. save the credentials in config/last_fm_credentials.json. An example formatting can be found in config/last_fm_credentials_EXAMPLE.json

3. configure config/config.yaml to your needs

4. from the base directory run: python3 data_collection/run.py

Note: the data collection can be interrupted at any point. It will pick up where it left when you restart the process.


### Data Cleaning

1. from the base directory run data_cleaning/run.py  - this does not overwrite anything in the original DB, so all changes can be reverted.



### Data Stream (optional)

For certain analysis such as the analysis of attachment kernels, it is convenient to have the listening data formatted in a time-ordered stream of events where each song listening event is related to a song, a real time, an intrinsic time and a timebin. To create a separate database that contains this stream, run data_stream/run.py
