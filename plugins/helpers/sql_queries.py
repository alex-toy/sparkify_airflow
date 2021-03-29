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
                SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
                FROM staging_events
                WHERE page='NextSong'
            ) events
            LEFT JOIN staging_songs songs
                ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    user_table_insert = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)
    
    
    staging_songs_table_create = (f"""
    CREATE TABLE IF NOT EXISTS staging_songs (
            "song_id" BIGINT IDENTITY(1,1), 
            "num_songs" INTEGER,
            "artist_id" TEXT,
            "artist_latitude" TEXT,
            "artist_longitude" TEXT,
            "artist_location" TEXT,
            "artist_name" TEXT,
            "title" TEXT,
            "duration" DOUBLE PRECISION,
            "year" INTEGER
        );
    """)
    
    
    staging_events_table_create = (f"""
        CREATE TABLE IF NOT EXISTS staging_events (
            "artist" TEXT,
            "auth" TEXT,
            "firstName" TEXT,
            "gender" CHAR,
            "itemInSession" INTEGER,
            "lastName" TEXT,
            "length" DOUBLE PRECISION,
            "level" TEXT,
            "location" TEXT,
            "method" TEXT,
            "page" TEXT,
            "registration" DOUBLE PRECISION,
            "sessionId" INTEGER,
            "song" TEXT,
            "status" INTEGER,
            "ts" BIGINT,
            "userAgent" TEXT,
            "userId" TEXT
        );
    """)
    
    
    
    
    
    
    
    
    