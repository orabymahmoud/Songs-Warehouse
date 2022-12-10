import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS stage_event"
staging_songs_table_drop = "DROP TABLE IF EXISTS stage_song"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS stage_event
    (
        artist          Text,
        auth            Text,
        first_name      Text,
        gender          Text,
        item_in_session INTEGER,
        last_name       Text,
        length          FLOAT4,
        level           Text,
        location        Text,
        method          Text,
        page            Text,
        registration    FLOAT8,
        session_id      INTEGER,
        song            Text,
        status          INTEGER,
        ts              BIGINT,
        user_agent      Text,
        user_id         Text
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS stage_song
    (
        song_id             Text,
        title               Text,
        duration            FLOAT4,
        year                SMALLINT,
        artist_id           Text,
        artist_name         Text,
        artist_latitude     REAL,
        artist_longitude    REAL,
        artist_location     Text,
        num_songs           INTEGER
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays
    (
        songplay_id    BIGINT IDENTITY(1, 1) PRIMARY KEY,
        start_time     TIMESTAMP NOT NULL SORTKEY,
        user_id        Text NOT NULL DISTKEY,
        level          Text,
        song_id        Text,
        artist_id      Text,
        session_id     INTEGER,
        location       Text,
        user_agent     Text
    ) diststyle key;
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users
    (
        user_id     Text PRIMARY KEY SORTKEY,
        first_name  Text,
        last_name   Text,
        gender      Text,
        level       Text
    ) diststyle all;
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs
    (
        song_id     Text PRIMARY KEY SORTKEY,
        title       Text,
        artist_id   Text DISTKEY,
        year        SMALLINT,
        duration    FLOAT4
    ) diststyle key;
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists
    (
        artist_id   Text PRIMARY KEY SORTKEY,
        name        Text,
        location    Text,
        latitude    FLOAT4,
        longitude   FLOAT4
    ) diststyle all;
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time
    (
        start_time  TIMESTAMP PRIMARY KEY SORTKEY,
        hour        SMALLINT,
        day         SMALLINT,
        week        SMALLINT,
        month       SMALLINT,
        year        SMALLINT DISTKEY,
        weekday     SMALLINT
    ) diststyle key;
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY {} FROM {}
    IAM_ROLE '{}'
    JSON {} region '{}';
""").format(
    'stage_event',
    config['S3']['LOG_DATA'],
    config['IAM_ROLE']['ARN'],
    config['S3']['LOG_JSONPATH'],
    config['CLUSTER']['REGION']
)

staging_songs_copy = ("""
    COPY {} FROM {}
    IAM_ROLE '{}'
    JSON 'auto' region '{}';
""").format(
    'stage_song',
    config['S3']['SONG_DATA'],
    config['IAM_ROLE']['ARN'],
    config['CLUSTER']['REGION']
)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) SELECT
        TIMESTAMP 'epoch' + (e.ts/1000 * INTERVAL '1 second'),
        e.user_id,
        e.level,
        s.song_id,
        s.artist_id,
        e.session_id,
        e.location,
        e.user_agent
    FROM stage_event e
    LEFT JOIN stage_song s ON
        e.song = s.title AND
        e.artist = s.artist_name AND
        ABS(e.length - s.duration) < 2
    WHERE
        e.page = 'NextSong'
""")

user_table_insert = ("""
    INSERT INTO users SELECT DISTINCT (user_id)
        user_id,
        first_name,
        last_name,
        gender,
        level
    FROM stage_event
""")

song_table_insert = ("""
    INSERT INTO songs SELECT DISTINCT (song_id)
        song_id,
        title,
        artist_id,
        year,
        duration
    FROM stage_song
""")

artist_table_insert = ("""
    INSERT INTO artists SELECT DISTINCT (artist_id)
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM stage_song
""")


time_table_insert = ("""
    INSERT INTO time
        WITH temp_time AS (SELECT TIMESTAMP 'epoch' + (ts/1000 * INTERVAL '1 second') as ts FROM stage_event)
        SELECT DISTINCT
        ts,
        extract(hour from ts),
        extract(day from ts),
        extract(week from ts),
        extract(month from ts),
        extract(year from ts),
        extract(weekday from ts)
        FROM temp_time
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
