# DROP TABLES

songplay_table_drop = "DROP TABLE IF  EXISTS songplays;"
user_table_drop = "DROP TABLE IF  EXISTS users;"
song_table_drop = "DROP TABLE IF  EXISTS songs;"
artist_table_drop = "DROP TABLE IF  EXISTS artists;"
time_table_drop = "DROP TABLE IF  EXISTS time;"
copy_drop = "DROP TABLE IF EXISTS copy;"
# CREATE TABLES

songplay_table_create = '''CREATE TABLE IF NOT EXISTS songplays(
songplay_id SERIAL    PRIMARY KEY ,
start_time  TIMESTAMP REFERENCES time NOT NULL,
user_id     INT       REFERENCES users NOT NULL, 
song_id     VARCHAR   REFERENCES songs, 
artist_id   VARCHAR   REFERENCES artists, 
session_id  INT       NOT NULL, 
location    VARCHAR,  
user_agent  VARCHAR, 
level       VARCHAR   NOT NULL);'''

user_table_create = '''CREATE TABLE IF NOT EXISTS users(
user_id    INT      PRIMARY KEY, 
first_name VARCHAR, 
last_name  VARCHAR, 
gender     CHAR, 
level      VARCHAR);'''

song_table_create = '''CREATE TABLE IF NOT EXISTS songs(
song_id   VARCHAR PRIMARY KEY, 
title     VARCHAR, 
artist_id VARCHAR, 
year      INT, 
duration  FLOAT);'''

artist_table_create = '''CREATE TABLE IF NOT EXISTS artists(
artist_id varchar PRIMARY KEY, 
name      varchar, 
location  varchar, 
latitude  float, 
longitude float);'''

time_table_create = '''CREATE TABLE IF NOT EXISTS time(
start_time timestamp PRIMARY KEY, 
hour       int, 
day        int, 
week       int, 
month      int, 
year       int, 
weekday    int);'''

# STAGGING TABLE

stagging_user = "CREATE TABLE copy AS TABLE users WITH NO DATA;"
stagging_song= "CREATE TABLE copy AS TABLE songs WITH NO DATA;"
stagging_artist = "CREATE TABLE copy AS TABLE artists WITH NO DATA;"
stagging_time = "CREATE TABLE copy AS TABLE time WITH NO DATA;"
stagging_songplay = "CREATE TABLE copy AS TABLE songplays WITH NO DATA;"

# INSERT MULTIPLE RECORDS 

songplay_table_insert ='''INSERT INTO songplays (start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)
select copy.start_time, copy.user_id, copy.level, copy.song_id, copy.artist_id, copy.session_id, copy.location, copy.user_agent 
FROM copy'''

user_table_insert = '''INSERT INTO users (user_id,first_name,last_name,gender,level)
select copy.user_id, copy.first_name, copy.last_name, copy.gender, copy.level 
FROM copy ON CONFLICT (user_id) 
DO UPDATE SET 
level=excluded.level;'''

song_table_insert = '''INSERT INTO songs (song_id,title,artist_id,year,duration)
select copy.song_id, copy.title, copy.artist_id, copy.year, copy.duration
FROM copy ON CONFLICT (song_id) 
DO UPDATE SET 
artist_id=excluded.artist_id, 
duration=excluded.duration;''' 

artist_table_insert = '''INSERT INTO artists (artist_id,name,location,latitude,longitude)
select copy.artist_id, copy.name, copy.location, copy.latitude, copy.longitude
FROM copy ON CONFLICT (artist_id) 
DO UPDATE SET 
location=excluded.location, 
latitude=excluded.latitude, 
longitude=excluded.longitude;'''

time_table_insert = '''INSERT INTO time (start_time,hour,day,week,month,year,weekday) 
select copy.start_time, copy.hour, copy.day, copy.week, copy.month,copy.year,copy.weekday 
FROM copy ON CONFLICT (start_time) 
DO NOTHING;'''

# COPY RECORDS

songplay_stagging_copy = """COPY copy (start_time,user_id,level,song_id,artist_id,session_id,location,user_agent) 
FROM '%s' DELIMITER ',' CSV HEADER;"""
user_stagging_copy = "COPY copy (user_id,first_name,last_name,gender,level) FROM '%s' DELIMITER ',' CSV HEADER;"
time_stagging_copy = "COPY copy (start_time,hour,day,week,month,year,weekday) FROM '%s' DELIMITER ',' CSV HEADER;"
song_stagging_copy = "COPY copy(song_id,title,artist_id,year,duration) FROM '%s' DELIMITER ',' CSV HEADER;"
artist_stagging_copy = "COPY copy (artist_id,name,location,latitude,longitude) FROM '%s' DELIMITER ',' CSV HEADER;"

# Update user

update_songs="UPDATE songs SET year = NULL WHERE year=0;"

# FIND SONGS

song_select = "SELECT song_id, songs.artist_id FROM songs JOIN artists ON songs.artist_id=artists.artist_id AND songs.title=%s AND artists.name=%s;"

# QUERY LISTS
create_table_queries = [user_table_create, song_table_create, artist_table_create, time_table_create,songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]