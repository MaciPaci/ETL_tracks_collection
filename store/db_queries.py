drop_tracks_collection_query = 'DROP TABLE IF EXISTS tracks_collection'
drop_unique_tracks_query = 'DROP TABLE IF EXISTS unique_tracks'
drop_tracks_play_history_query = 'DROP TABLE IF EXISTS tracks_play_history'

create_tracks_collection_table_query = '''
CREATE TABLE tracks_collection(
    track_id VARCHAR(255) PRIMARY KEY,
    artist VARCHAR(255),
    title VARCHAR(255),
    play_count INTEGER DEFAULT 0
)
'''

create_unique_tracks_table_query = '''
CREATE TABLE unique_tracks(
    play_id VARCHAR(255),
    track_id VARCHAR(255) PRIMARY KEY,
    artist VARCHAR(255),
    title VARCHAR(255)
)
'''

create_tracks_play_history_table_query = '''
CREATE TABLE tracks_play_history(
    id INTEGER PRIMARY KEY,
    user_id VARCHAR(255),
    track_id VARCHAR(255),
    play_date TIMESTAMP
)
'''

fill_unique_tracks_query = 'INSERT INTO unique_tracks VALUES (?, ?, ?, ?)'

fill_tracks_play_history_query = 'INSERT INTO tracks_play_history (user_id, track_id, play_date) VALUES (?, ?, ?)'

fill_tracks_collection_query = '''
    INSERT INTO tracks_collection
    SELECT u.track_id, u.artist, u.title, t.count FROM unique_tracks AS u
    LEFT JOIN (SELECT track_id, COUNT(*) as count FROM tracks_play_history GROUP BY track_id) as t
    ON u.track_id = t.track_id ORDER BY t.count DESC
    '''

get_most_listened_artist_query = '''
    SELECT artist, SUM(play_count) FROM tracks_collection 
    GROUP BY artist ORDER BY SUM(play_count) DESC LIMIT 1
    '''

get_most_played_songs_query = 'SELECT title, play_count FROM tracks_collection ORDER BY play_count DESC'
