import argparse
from sqlite3 import connect

create_tracks_collection_table = '''
CREATE TABLE tracks_collection(
    track_id VARCHAR(255) PRIMARY KEY,
    artist VARCHAR(255),
    title VARCHAR(255),
    play_count INTEGER
)
'''

create_unique_tracks_table = '''
CREATE TABLE unique_tracks(
    play_id VARCHAR(255),
    track_id VARCHAR(255) PRIMARY KEY,
    artist VARCHAR(255),
    title VARCHAR(255)
)
'''

create_tracks_play_history_table = '''
CREATE TABLE tracks_play_history(
    user_id VARCHAR(255) PRIMARY KEY,
    track_id VARCHAR(255),
    play_date TIMESTAMP
)
'''

tables_to_create = [
    create_tracks_collection_table,
    create_unique_tracks_table,
    create_tracks_play_history_table
]

tables = [
    "tracks_collection",
    "unique_tracks",
    "tracks_play_history"
]


def create_tables(db_cursor):
    for table in tables_to_create:
        db_cursor.execute(table)


def remove_tables(db_cursor):
    db_cursor.execute('DROP TABLE IF EXISTS tracks_collection')
    db_cursor.execute('DROP TABLE IF EXISTS unique_tracks')
    db_cursor.execute('DROP TABLE IF EXISTS tracks_play_history')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tracks", type=str, required=True)
    # parser.add_argument("--triplets", type=str, required=True)
    args = parser.parse_args()

    tracks_path = args.tracks
    # triplets_path = args.triplets

    with connect('track_library.db') as db_connector:
        db_cursor = db_connector.cursor()
        remove_tables(db_cursor)
        create_tables(db_cursor)

        with open(tracks_path, 'r', encoding='latin-1') as f:
            for line in f:
                print(line)
                line = line.strip()
                line = line.split('<SEP>')
                print(line)


if __name__ == '__main__':
    main()
