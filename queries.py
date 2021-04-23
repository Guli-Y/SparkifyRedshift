import configparser

config=configparser.ConfigParser()
config.read('config.ini')

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events CASCADE;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs CASCADE;"
songplays_table_drop = "DROP TABLE IF EXISTS songplays CASCADE;"
users_table_drop = "DROP TABLE IF EXISTS users CASCADE;"
songs_table_drop = "DROP TABLE IF EXISTS songs CASCADE;"
artists_table_drop = "DROP TABLE IF EXISTS artists CASCADE;"
time_table_drop = "DROP TABLE IF EXISTS time CASCADE;"

# CREATE TABLES
staging_events_table_create = (
    """ CREATE TABLE staging_events (
            artist VARCHAR,
            auth VARCHAR,
            first_name VARCHAR,
            gender VARCHAR(1),
            item_in_session SMALLINT,
            last_name VARCHAR,
            length FLOAT,
            level VARCHAR,
            location VARCHAR,
            method VARCHAR,
            page VARCHAR,
            registration FLOAT,
            session_id SMALLINT,
            song VARCHAR,
            status SMALLINT,
            ts BIGINT,
            user_agent VARCHAR,
            user_id INT);"""
)

staging_songs_table_create = (
    """ CREATE TABLE staging_songs(
            num_song SMALLINT,
            artist_id VARCHAR,
            artist_latitude FLOAT,
            artist_longitude FLOAT,
            artist_location VARCHAR,
            artist_name VARCHAR,
            song_id VARCHAR,
            title VARCHAR,
            duration FLOAT,
            year INT);""" 
)

songplays_table_create = (
    """ CREATE TABLE songplays (
            songplay_id INT IDENTITY (0,1) PRIMARY KEY, 
            start_time TIMESTAMP NOT NULL REFERENCES time (start_time), 
            user_id SMALLINT NOT NULL REFERENCES users (user_id),
            level VARCHAR, 
            song_id VARCHAR NOT NULL REFERENCES songs (song_id), 
            artist_id VARCHAR NOT NULL REFERENCES artists (artist_id), 
            session_id SMALLINT, 
            location VARCHAR, 
            user_agent VARCHAR
            );"""
)

users_table_create = (
    """ CREATE TABLE users(
            user_id INT PRIMARY KEY, 
            first_name VARCHAR, 
            last_name VARCHAR, 
            gender VARCHAR(1), 
            level VARCHAR
            );"""
)

songs_table_create = (
    """CREATE TABLE songs (
            song_id VARCHAR PRIMARY KEY, 
            title VARCHAR, 
            artist_id VARCHAR NOT NULL REFERENCES artists (artist_id), 
            year SMALLINT, 
            duration FLOAT
            );"""
)

artists_table_create = (
    """CREATE TABLE artists (
            artist_id VARCHAR PRIMARY KEY, 
            name VARCHAR, 
            location VARCHAR, 
            latitude FLOAT, 
            longitude FLOAT
            );"""
)

time_table_create = (
    """CREATE TABLE time (
            start_time timestamp PRIMARY KEY, 
            hour SMALLINT, 
            day SMALLINT, 
            week SMALLINT, 
            month SMALLINT, 
            year SMALLINT, 
            weekday SMALLINT
            );"""
)

# COPY DATA
staging_events_copy = (
    f"""
    COPY staging_events 
    FROM {config['S3']['LOG_DATA']}
    CREDENTIALS 'aws_iam_role={config['IAM_ROLE']['ROLE_ARN']}'
    REGION 'us-west-2'
    FORMAT AS JSON {config['S3']['LOG_JSONPATH']};"""
)

staging_songs_copy = (
    f"""
    COPY staging_songs 
    FROM {config['S3']['SONG_DATA']}
    CREDENTIALS 'aws_iam_role={config['IAM_ROLE']['ROLE_ARN']}'
    json 'auto';"""
)

# INSERT DATA

songplays_table_insert = (
    """INSERT INTO songplays(
            start_time, 
            user_id, 
            level, 
            session_id, 
            location, 
            user_agent, 
            song_id, 
            artist_id
            )
       SELECT
           DISTINCT(TIMESTAMP 'epoch' + INTERVAL '1 second' * ts/1000) AS start_time,
           e.user_id,
           e.level,
           e.session_id,
           e.location,
           e.user_agent,
           s.song_id,
           s.artist_id
       FROM staging_events e
           LEFT JOIN staging_songs s 
               ON e.song=s.title 
               AND e.artist=s.artist_name
               AND e.length=s.duration
       WHERE page='NextSong'
           AND user_id IS NOT NULL
           AND song_id IS NOT NULL
           AND artist_id IS NOT NULL;"""
)

users_table_insert = (
    """INSERT INTO users (
            user_id, 
            first_name, 
            last_name, 
            gender, 
            level
            )
           SELECT
               user_id,
               first_name,
               last_name,
               gender,
               level
           FROM staging_events
           WHERE page='NextSong' 
               AND user_id IS NOT NULL
               AND user_id NOT IN (SELECT DISTINCT(user_id) FROM users);
    """
)

songs_table_insert = (
    """INSERT INTO songs (
            song_id, 
            title, 
            artist_id,
            year, 
            duration
            )
           SELECT
               song_id,
               title,
               artist_id,
               year,
               duration
           FROM staging_songs
           WHERE song_id IS NOT NULL
               AND song_id NOT IN (SELECT DISTINCT(song_id) FROM songs);
    """
)

artists_table_insert = (
    """INSERT INTO artists (
            artist_id, 
            name, 
            location, 
            latitude, 
            longitude
            )
          SELECT
              artist_id,
              artist_name,
              artist_location,
              artist_latitude,
              artist_longitude
          FROM staging_songs
          WHERE artist_id IS NOT NULL
              AND artist_id NOT IN (SELECT DISTINCT(artist_id) FROM artists);
  """
)


time_table_insert = (
    """INSERT INTO time (
            start_time, 
            hour, 
            day, 
            week, 
            month, 
            year, 
            weekday
            )
           SELECT
               start_time,
               DATE_PART('hour', start_time) AS hour,
               DATE_PART('day', start_time) AS day,
               DATE_PART('week', start_time) AS week,
               DATE_PART('month', start_time) AS month,
               DATE_PART('year', start_time) AS year,
               DATE_PART('dow', start_time) AS weekday
           FROM (SELECT 
                   DISTINCT(TIMESTAMP 'epoch' + INTERVAL '1 second' * ts/1000) AS start_time
                 FROM staging_events
                 WHERE page='NextSong');
  """
)

# QUERY LISTS

drop_table_queries = {'staging_events' : staging_events_table_drop,
                      'staging_songs' : staging_songs_table_drop, 
                      'users': users_table_drop,  
                      'artists': artists_table_drop, 
                      'time': time_table_drop,
                      'songs': songs_table_drop,
                      'songplays': songplays_table_drop
                     }

create_table_queries = {'staging_events' : staging_events_table_create,
                        'staging_songs' : staging_songs_table_create, 
                        'users': users_table_create,  
                        'artists': artists_table_create, 
                        'time': time_table_create,
                        'songs': songs_table_create,
                        'songplays': songplays_table_create
                       }

copy_table_queries = {'staging_events': staging_events_copy,
                      'staging_songs' : staging_songs_copy
                     }

insert_table_queries = {'songplays': songplays_table_insert,
                        'users': users_table_insert,
                        'songs': songs_table_insert,
                        'artists': artists_table_insert,
                        'time': time_table_insert
                       }

