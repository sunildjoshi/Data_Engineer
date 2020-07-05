class SqlQueries:
    songplay_table_insert = ("""
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
            FROM (
                SELECT 
                    TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
                FROM staging_events
                WHERE page='NextSong'
                AND ts is NOT NULL 
                AND sessionId is NOT NULL 
                AND userId is NOT NULL
                AND level is NOT NULL
                ) events
            LEFT JOIN staging_songs songs
                ON (events.song = songs.title
                    AND events.artist = songs.artist_name
                    AND events.length = songs.duration
                    )
    """)

    user_table_insert = ("""
        SELECT userid, firstname, lastname, gender, level
        FROM (SELECT userId, firstName, lastName, gender, level,ts,
               ROW_NUMBER () OVER 
               (PARTITION BY userId ORDER BY ts DESC) AS rownum
                FROM staging_events
                WHERE page='NextSong' 
                AND ts is NOT NULL 
                AND sessionId is NOT NULL 
                AND userId is NOT NULL
                AND level is NOT NULL) groups
                WHERE groups.rownum = 1
    """)

    song_table_insert = ("""
        SELECT song_id, title, artist_id, year, duration
        FROM (SELECT song_id, title, artist_id, year, duration, 
                      ROW_NUMBER () OVER (PARTITION BY song_id) AS rownum
              FROM staging_songs) groups
              WHERE groups.rownum = 1
    """)

    artist_table_insert = ("""
        SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM (SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude, 
                     ROW_NUMBER () OVER (PARTITION BY artist_id) AS rownum
              FROM staging_songs
              WHERE artist) groups
              WHERE groups.rownum = 1
    """)

    time_table_insert = ("""
        SELECT distinct start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)