import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = ("""CREATE TABLE staging_events(
    event_id int IDENTITY(0,1) PRIMARY KEY,
    artist varchar(200),
    auth varchar(100),
    first_name varchar(200),
    gender varchar,
    itemInSession int,
    last_name varchar(200),
    length	double precision, 
    level varchar(50),
    location varchar(200),	
    method varchar,
    page varchar(100),	
    registration varchar(100),	
    session_id	bigint,
    title varchar(200),
    status int,	
    ts varchar(100),
    user_agent text,	
    user_id varchar(100))
""")

staging_songs_table_create = ("""CREATE TABLE staging_songs(
    song_id varchar(100) PRIMARY KEY,
    num_songs int,
    artist_id varchar(100),
    latitude double precision,
    longitude double precision,
    location varchar(200),
    name varchar(200),
    title varchar(200),
    duration double precision,
    year int)
""")

songplay_table_create = ( """CREATE TABLE songplays(
    songplay_id INT IDENTITY(0,1) PRIMARY KEY,
    start_time timestamp,
    user_id varchar(50),
    level varchar(50),
    song_id varchar(100),
    artist_id varchar(100),
    session_id bigint,
    location varchar(255),
    user_agent text)
""")

user_table_create = ("""CREATE TABLE users(
    user_id VARCHAR PRIMARY KEY,
    first_name varchar(150),
    last_name varchar(150),
    gender varchar,
    level varchar)
""")

song_table_create = ("""CREATE TABLE songs(
    song_id varchar(100) PRIMARY KEY,
    title varchar(200),
    artist_id varchar(100),
    year int,
    duration double precision)
""")

artist_table_create = ("""CREATE TABLE artists(
    artist_id varchar(100) PRIMARY KEY,
    name varchar(200),
    location varchar(20),
    latitude double precision,
    longitude double precision)
""")

time_table_create = ("""CREATE TABLE time(
    start_time timestamp PRIMARY KEY,
    hour int,
    day int,
    week int,
    month int,
    year int,
    weekday int)
""")

# STAGING TABLES

staging_events_copy = ("""copy {} from '{}'
 credentials 'aws_iam_role={}'
 region 'us-west-2' 
 JSON '{}' """).format('staging_events',config.get('S3','LOG_DATA'),
                        config.get('IAM_ROLE', 'ARN'),
                       config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""copy {} from '{}'
    credentials 'aws_iam_role={}'
    region 'us-west-2' 
    JSON 'auto'
    """).format('staging_songs',config.get('S3','SONG_DATA'), 
                config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
    SELECT DISTINCT 
        TIMESTAMP 'epoch' + se.ts/1000 *INTERVAL '1 second' as start_time, 
        se.user_id, 
        se.level,
        ss.song_id,
        ss.artist_id,
        se.session_id,
        se.location,
        se.user_agent
    FROM staging_events se, staging_songs ss
    WHERE se.page = 'NextSong'
""")
 
user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)  
    SELECT DISTINCT 
        user_id,
        first_name,
        last_name,
        gender, 
        level
    FROM staging_events
    WHERE page = 'NextSong'
    AND user_id NOT IN (SELECT DISTINCT user_id FROM users)
""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration) 
    SELECT DISTINCT 
        song_id, 
        title,
        artist_id,
        year,
        duration
    FROM staging_songs
    WHERE song_id NOT IN (SELECT DISTINCT song_id FROM songs)
""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude) 
    SELECT DISTINCT 
        artist_id,
        name,
        location,
        latitude,
        longitude
    FROM staging_songs
    WHERE artist_id NOT IN (SELECT DISTINCT artist_id FROM artists)
""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT 
        start_time, 
        EXTRACT(hr from start_time) AS hour,
        EXTRACT(d from start_time) AS day,
        EXTRACT(w from start_time) AS week,
        EXTRACT(mon from start_time) AS month,
        EXTRACT(yr from start_time) AS year, 
        EXTRACT(weekday from start_time) AS weekday 
    FROM (
    	SELECT DISTINCT timestamp 'epoch' + ts/1000 *interval '1 second' as start_time 
        FROM staging_events)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]