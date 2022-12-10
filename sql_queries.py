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
        artist          varchar(100) NOT NULL,
        auth            varchar(100) NOT NULL,
        first_name      varchar(100) NOT NULL,
        gender          varchar(100) NOT NULL,
        item_in_session INTEGER,
        last_name       varchar(100) NOT NULL,
        length          FLOAT4,
        level           varchar(100) NOT NULL,
        location        varchar(100) NOT NULL,
        method          varchar(100) NOT NULL,
        page            varchar(100) NOT NULL,
        registration    FLOAT8,
        session_id      INTEGER,
        song            varchar(100) NOT NULL,
        status          INTEGER,
        ts              BIGINT,
        user_agent      varchar(100) NOT NULL,
        user_id         varchar(100) NOT NULL
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS stage_song
    (
        song_id             varchar(100) NOT NULL,
        title               varchar(100) NOT NULL,
        duration            FLOAT4,
        year                SMALLINT,
        artist_id           varchar(100) NOT NULL,
        artist_name         varchar(100) NOT NULL,
        artist_latitude     REAL,
        artist_longitude    REAL,
        artist_location     varchar(100) NOT NULL,
        num_songs           INTEGER
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays
    (
        songplay_id    BIGINT IDENTITY(1, 1) PRIMARY KEY,
        start_time     TIMESTAMP NOT NULL SORTKEY,
        user_id        varchar(100) NOT NULL NOT NULL DISTKEY,
        level          varchar(100) NOT NULL,
        song_id        varchar(100) NOT NULL,
        artist_id      varchar(100) NOT NULL,
        session_id     INTEGER,
        location       varchar(100) NOT NULL,
        user_agent     varchar(100) NOT NULL
    ) diststyle key;
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users
    (
        user_id     varchar(100) NOT NULL PRIMARY KEY SORTKEY,
        first_name  varchar(100) NOT NULL,
        last_name   varchar(100) NOT NULL,
        gender      varchar(100) NOT NULL,
        level       varchar(100) NOT NULL
    ) diststyle all;
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs
    (
        song_id     varchar(100) NOT NULL PRIMARY KEY SORTKEY,
        title       varchar(100) NOT NULL,
        artist_id   varchar(100) NOT NULL DISTKEY,
        year        SMALLINT,
        duration    FLOAT4
    ) diststyle key;
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists
    (
        artist_id   varchar(100) NOT NULL PRIMARY KEY SORTKEY,
        name        varchar(100) NOT NULL,
        location    varchar(100) NOT NULL,
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
