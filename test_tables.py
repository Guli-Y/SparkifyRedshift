import pytest
import configparser
import psycopg2

config = configparser.ConfigParser()
config.read('config.ini')

conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
cur = conn.cursor()

def test_staging_songs():
    query = "SELECT count(*) FROM staging_songs;"
    cur.execute(query)
    result = cur.fetchall()
    assert result[0][0] == 14896

def test_staging_events():
    query = "SELECT count(*) FROM staging_events WHERE page='NextSong';"
    cur.execute(query)
    result = cur.fetchall()
    assert result[0][0] == 6820

def test_songplays():
    query = "SELECT count(*) FROM songplays;"
    cur.execute(query)
    result = cur.fetchall()
    assert result[0][0] == 6820
    query = "SELECT artist_id, user_id FROM songplays ORDER BY songplay_id DESC LIMIT 1;"
    cur.execute(query)
    result = cur.fetchall()
    assert result[0][0] == 'ARL26PR1187FB576E5'
    assert result[0][1] == 73

def test_songs():
    query = "SELECT title FROM songs WHERE artist_id='ARL26PR1187FB576E5' ORDER BY song_id LIMIT 1;"
    cur.execute(query)
    result = cur.fetchall()
    assert result[0][0] == 'Double Feature'