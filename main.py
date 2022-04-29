import argparse
from sqlite3 import connect, IntegrityError
from timeit import default_timer as timer

from store.db_queries import *

tables_to_create = [
    create_tracks_collection_table_query,
    create_unique_tracks_table_query,
    create_tracks_play_history_table_query
]


def create_tables(db_cursor):
    print("Creating database tables...")
    for table in tables_to_create:
        db_cursor.execute(table)
        print("Table created")


def remove_tables(db_cursor):
    db_cursor.execute(drop_tracks_collection_query)
    db_cursor.execute(drop_unique_tracks_query)
    db_cursor.execute(drop_tracks_play_history_query)


def main():
    program_start_time = timer()
    parser = argparse.ArgumentParser()
    parser.add_argument("--tracks", type=str, required=True)
    parser.add_argument("--triplets", type=str, required=True)
    args = parser.parse_args()

    tracks_path = args.tracks
    triplets_path = args.triplets

    db_connector = connect('track_library.db')
    db_cursor = db_connector.cursor()
    remove_tables(db_cursor)
    create_tables(db_cursor)

    rows_omitted = 0

    print("Processing", tracks_path, "file...")
    tracks_start_time = timer()
    with open(tracks_path, 'r', encoding='latin-1') as f:
        c = 0
        for line in f:
            tracks = line.strip().split('<SEP>')
            if len(tracks) == 4:
                try:
                    db_cursor.execute(fill_unique_tracks_query, tracks)
                except IntegrityError:
                    rows_omitted += 1
                    continue
            else:
                rows_omitted += 1
            c += 1
    db_connector.commit()
    f.close()
    tracks_end_time = timer()
    print("Total", c, "records inserted successfully into unique_tracks table")
    print("Total", rows_omitted, "records omitted from unique_tracks table")
    print("File", tracks_path, "processing took", tracks_end_time - tracks_start_time, "seconds to complete")

    rows_omitted = 0

    print("Processing", triplets_path, "file...")
    triplets_start_time = timer()
    with open(triplets_path, 'r', encoding='latin-1') as f:
        c = 0
        for line in f:
            triplets = line.strip().split('<SEP>')
            if len(triplets) == 3:
                db_cursor.execute(fill_tracks_play_history_query, triplets)
            else:
                rows_omitted += 1
            c += 1
    db_connector.commit()
    f.close()
    triplets_end_time = timer()
    print("Total", c, "records inserted successfully into tracks_play_history table")
    print("Total", rows_omitted, "records omitted from tracks_play_history table")
    print("File", triplets_path, "processing took", triplets_end_time - triplets_start_time, "seconds to complete")

    db_cursor.execute(fill_tracks_collection_query)
    db_connector.commit()

    db_cursor.execute(get_most_listened_artist_query)
    (artist, play_count) = db_cursor.fetchone()
    print("Artist with the most played songs in the collection:", artist, "; songs played:", play_count)

    db_cursor.execute(get_most_played_songs_query)
    list_of_songs_with_play_count = db_cursor.fetchmany(5)
    print("Five most played songs in the collection:")
    for index, tuple in enumerate(list_of_songs_with_play_count):
        song = tuple[0]
        play_count = tuple[1]
        print(song, "was played", play_count, "times")

    db_cursor.close()
    db_connector.close()
    program_end_time = timer()
    print("Program runtime took", program_end_time - program_start_time, "seconds to finish ")


if __name__ == '__main__':
    main()
