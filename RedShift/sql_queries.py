
import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS stagging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS stagging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES
staging_events_table_create= """
CREATE TABLE IF NOT EXISTS stagging_events(
artist        VARCHAR,
auth          VARCHAR,
firstName     VARCHAR, 
gender        CHAR,
itemInSession INTEGER,
lastName      VARCHAR,
length        FLOAT,
level         VARCHAR,
location      VARCHAR,
method        VARCHAR,
page          VARCHAR,
registration  FLOAT,
sessionId     INTEGER,
song          VARCHAR,
status        INTEGER,
ts            BIGINT,
userAgent     VARCHAR,
userId        INTEGER);"""

staging_songs_table_create = """
CREATE TABLE IF NOT EXISTS stagging_songs(
num_songs        INTEGER,
artist_id        VARCHAR,
artist_latitude  FLOAT,
artist_longitude FLOAT,
artist_location  VARCHAR,
artist_name      VARCHAR,
song_id          VARCHAR,
title            VARCHAR,
duration         FLOAT,
year             INTEGER);"""

songplay_table_create = """CREATE TABLE IF NOT EXISTS songplays(
songplay_id INTEGER   IDENTITY(1,1) PRIMARY KEY,
start_time  TIMESTAMP NOT NULL SORTKEY DISTKEY,
user_id     INTEGER   NOT NULL, 
song_id     VARCHAR,  
artist_id   VARCHAR, 
session_id  INTEGER  NOT NULL, 
location    VARCHAR,  
user_agent  VARCHAR, 
level       VARCHAR  NOT NULL);"""

user_table_create = """CREATE TABLE IF NOT EXISTS users(
user_id    INTEGER  PRIMARY KEY SORTKEY, 
first_name VARCHAR, 
last_name  VARCHAR, 
gender     CHAR, 
level      VARCHAR)
DISTSTYLE ALL;"""

time_table_create = """CREATE TABLE IF NOT EXISTS time(
start_time TIMESTAMP PRIMARY KEY SORTKEY DISTKEY, 
hour       INTEGER, 
day        INTEGER, 
week       INTEGER, 
month      INTEGER, 
year       INTEGER, 
weekday    VARCHAR);"""

artist_table_create = """CREATE TABLE IF NOT EXISTS artists(
artist_id VARCHAR PRIMARY KEY SORTKEY, 
name      VARCHAR, 
location  VARCHAR, 
latitude  FLOAT, 
longitude FLOAT)
DISTSTYLE ALL;"""

song_table_create = """CREATE TABLE IF NOT EXISTS songs(
song_id   VARCHAR PRIMARY KEY SORTKEY, 
title     VARCHAR,
artist_id VARCHAR, 
year      INTEGER, 
duration  FLOAT)
DISTSTYLE ALL;"""

# STAGING TABLES

staging_events_copy = ("""COPY stagging_events 
                          FROM {} 
                          iam_role {} 
                          region 'us-west-2'
                          FORMAT AS json {}; """).format(config['S3']['LOG_DATA'],
                                                          config['IAM_ROLE']['ARN'],
                                                          config['S3']['LOG_JSONPATH'] )

staging_songs_copy = ("""COPY stagging_songs 
                         FROM {} 
                         iam_role {}
                         region 'us-west-2'
                         json 'auto';""").format(config['S3']['SONG_DATA'],
                                                 config['IAM_ROLE']['ARN'])

# DELETE RECORDS
stagging_events_record_delete="""DELETE FROM stagging_events 
                                 WHERE page!='NextSong' 
                                 OR ts is NULL 
                                 OR sessionId is NULL 
                                 OR userId is NULL
                                 OR level is NULL;"""

# FINAL TABLES

songplay_table_insert = """INSERT INTO songplays
                           (start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)
                           SELECT DISTINCT TIMESTAMP 'epoch' + (e.ts / 1000) * INTERVAL '1 second' as start_time, 
                           e.userId, e.level, 
                           s.song_id, s.artist_id, 
                           e.sessionId, e.location, e.userAgent 
                           FROM stagging_events e LEFT JOIN stagging_songs s
                           ON (e.song=s.title AND 
			       e.artist = s.artist_name AND 
			       e.length = s.duration
                    ));"""

user_table_insert = """INSERT INTO users (user_id,first_name,last_name,gender,level)
                       SELECT distinct userId, firstName, lastName, gender, level 
                       FROM ( 
                           SELECT userId, firstName, lastName, gender, level,ts,
                           ROW_NUMBER () OVER 
                           (PARTITION BY userId ORDER BY ts DESC) AS rownum
                            FROM stagging_events) groups
                       WHERE groups.rownum = 1;"""

song_table_insert = """INSERT INTO songs (song_id,title,artist_id,year,duration)
                       SELECT song_id, title, artist_id, year, duration 
                       FROM (
                            SELECT song_id, title, artist_id, year, duration, 
                            ROW_NUMBER () OVER (PARTITION BY song_id) AS rownum
                            FROM stagging_songs) groups
                       WHERE groups.rownum = 1;"""

artist_table_insert = """INSERT INTO artists (artist_id,name,location,latitude,longitude)
                         SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude 
                         FROM (
                             SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude, 
                             ROW_NUMBER () OVER (PARTITION BY artist_id) AS rownum
                             FROM stagging_songs) groups
                         WHERE groups.rownum = 1;"""

time_table_insert = """INSERT INTO time (start_time, hour, day, week, month, year, weekday) 
                       SELECT start_time, 
                       EXTRACT(HOUR FROM start_time),
                       EXTRACT(DAY FROM start_time), 
                       EXTRACT(WEEKS FROM start_time),
                       EXTRACT(MONTH FROM start_time),
                       EXTRACT(YEAR FROM start_time),
                       to_char(start_time, 'Day')
                       FROM (
                           SELECT DISTINCT TIMESTAMP 'epoch' + (ts / 1000) * INTERVAL '1 second' as start_time 
                           FROM stagging_events) t;"""

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
