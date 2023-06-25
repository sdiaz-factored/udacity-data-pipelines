class SqlQueries:
    songplay_table_insert = """
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
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """

    user_table_insert = """
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """

    song_table_insert = """
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """

    artist_table_insert = """
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """

    time_table_insert = """
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """


class DataQualityTest:
    tests = [
        {
            "description": "Table is not empty",
            "table": "staging_events",
            "sql": "SELECT  count(*) FROM staging_events where userid is null",
            "expected": 1,
            "type": "general",
        },
        {
            "description": "Table is not empty",
            "table": "staging_events",
            "sql": "SELECT count(*) FROM staging_events",
            "expected": 1,
            "type": "count",
        },
        {
            "description": "Table is not empty",
            "table": "staging_songs",
            "sql": "SELECT count(*) FROM staging_songs",
            "expected": 1,
            "type": "count",
        },
        {
            "description": "Table is not empty",
            "table": "songplays",
            "sql": "SELECT count(*) FROM songplays",
            "expected": 1,
            "type": "count",
        },
        {
            "description": "Table is not empty",
            "table": "users",
            "sql": "SELECT count(*) FROM users",
            "expected": 1,
            "type": "count",
        },
        {
            "description": "Table is not empty",
            "table": "songs",
            "sql": "SELECT count(*) FROM songs",
            "expected": 1,
            "type": "count",
        },
        {
            "description": "Table is not empty",
            "table": "artists",
            "sql": "SELECT count(*) FROM artists",
            "expected": 1,
            "type": "count",
        },
        {
            "description": "Table is not empty",
            "table": "time",
            "sql": "SELECT count(*) FROM time",
            "expected": 1,
            "type": "count",
        },
        {
            "description": "Table is not empty",
            "table": "time",
            "sql": "SELECT count(*) FROM time",
            "expected": 1,
            "type": "count",
        },
    ]
