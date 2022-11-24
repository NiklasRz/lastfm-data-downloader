from queries import dbq as dbq
import time


"""
Just for quickly checking the progress of the data collection while it's still running.
"""

sleep_time = 100
print(f"Counting for {sleep_time} seconds")

songs, listens, artists, tags, missing_tags = dbq.get_data_stats()
time.sleep(sleep_time)
songs2, listens2, artists2, tags2, missing_tags = dbq.get_data_stats()

print(f"Rate per hour:\n    Songs: {int((songs2 - songs) / 100 * 3600)}\n    Listens: {int((listens2 - listens) / 100 * 3600)}\n    Artists: {int((artists2 - artists) / 100 * 3600)}\n    Tags: {int((tags2 - tags) / 100 * 3600)}")

print(f"\nTotal:\n    Songs: {songs2}\n    Listens: {listens2}\n    Artists: {artists2}\n    Tags: {tags2}")

print(f"\nMissing:\n    Tags: {missing_tags}")
