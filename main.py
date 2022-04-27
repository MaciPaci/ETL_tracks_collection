import argparse
from sqlite3 import connect, IntegrityError
from timeit import default_timer as timer

create_tracks_collection_table = '''
CREATE TABLE tracks_collection(
    track_id VARCHAR(255) PRIMARY KEY,
    artist VARCHAR(255),
    title VARCHAR(255),
    play_count INTEGER DEFAULT 0
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
    id INTEGER PRIMARY KEY,
    user_id VARCHAR(255),
    track_id VARCHAR(255),
    play_date TIMESTAMP
)
'''

tables_to_create = [
    create_tracks_collection_table,
    create_unique_tracks_table,
    create_tracks_play_history_table
]


def create_tables(db_cursor):
    print("Creating database tables...")
    for table in tables_to_create:
        db_cursor.execute(table)
        print("Table created")


def remove_tables(db_cursor):
    db_cursor.execute('DROP TABLE IF EXISTS tracks_collection')
    db_cursor.execute('DROP TABLE IF EXISTS unique_tracks')
    db_cursor.execute('DROP TABLE IF EXISTS tracks_play_history')


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
                    db_cursor.execute('INSERT INTO unique_tracks VALUES (?, ?, ?, ?)', tracks)
                except IntegrityError as e:
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
                db_cursor.execute('INSERT INTO tracks_play_history (user_id, track_id, play_date) VALUES (?, ?, ?)',
                                  triplets)
            else:
                rows_omitted += 1
            c += 1
    db_connector.commit()
    f.close()
    triplets_end_time = timer()
    print("Total", c, "records inserted successfully into tracks_play_history table")
    print("Total", rows_omitted, "records omitted from tracks_play_history table")
    print("File", triplets_path, "processing took", triplets_end_time - triplets_start_time, "seconds to complete")

    db_cursor.execute('''
    INSERT INTO tracks_collection
    SELECT u.track_id, u.artist, u.title, t.count FROM unique_tracks AS u
    LEFT JOIN (SELECT track_id, COUNT(*) as count FROM tracks_play_history GROUP BY track_id) as t
    ON u.track_id = t.track_id ORDER BY t.count DESC
    ''')
    db_connector.commit()

    db_cursor.execute('''
    SELECT artist, SUM(play_count) FROM tracks_collection 
    GROUP BY artist ORDER BY SUM(play_count) DESC LIMIT 1
    ''')
    (artist, play_count) = db_cursor.fetchone()
    print("Artist with the most played songs in the collection:", artist, "songs played:", play_count)

    db_cursor.execute('''
    SELECT title, play_count FROM tracks_collection ORDER BY play_count DESC
    ''')
    list_of_songs_with_play_count = db_cursor.fetchmany(5)
    songs, play_count = zip(*list_of_songs_with_play_count)
    print("Five most played songs in the collection:")
    for song, played in songs, play_count:
        print(song, "was played", played, "times")

    db_cursor.close()
    db_connector.close()
    program_end_time = timer()
    print("Program runtime took", program_end_time - program_start_time, "seconds to finish ")


if __name__ == '__main__':
    main()
