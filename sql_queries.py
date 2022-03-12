import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "drop table if exists staging_events;"
staging_songs_table_drop = "drop table if exists staging_songs;"
songplay_table_drop = "drop table if exists songplay;"
users_table_drop = "drop table if exists users;"
song_table_drop = "drop table if exists song;"
artist_table_drop = "drop table if exists artist;"
time_table_drop = "drop table if exists time;"

# CREATE TABLES

staging_events_table_create = ("""
    create table if not exists staging_events (
        artist         VARCHAR,
        auth           VARCHAR,
        firstName      VARCHAR,
        gender         VARCHAR,
        itemInSession  INT,
        lastname       VARCHAR,
        length         DECIMAL,
        level          VARCHAR,
        location       VARCHAR,
        method         VARCHAR,
        page           VARCHAR,
        registration   DECIMAL,
        sessionId      INT,
        song           VARCHAR,
        status         INT,
        ts             BIGINT,
        userAgent      VARCHAR,
        userId         VARCHAR
    );
""")

staging_songs_table_create = ("""
    create table if not exists staging_songs (
        num_songs        INT,
        artist_id        VARCHAR,
        artist_latitude  DECIMAL,
        artist_longitude DECIMAL,
        artist_location  VARCHAR,
        artist_name      VARCHAR,
        song_id          VARCHAR,
        title            VARCHAR,
        duration         DECIMAL,
        year             INT
    );
""")

songplay_table_create = ("""
    create table if not exists songplay (
        songplay_id    BIGINT IDENTITY(0,1) PRIMARY KEY, 
        start_time     BIGINT NOT NULL, 
        user_id        INT, 
        level          VARCHAR, 
        song_id        VARCHAR, 
        artist_id      VARCHAR, 
        session_id     INT, 
        location       VARCHAR, 
        user_agent     VARCHAR
    );
""")

users_table_create = ("""
    create table if not exists users (
        user_id        BIGINT PRIMARY KEY,
        first_name     VARCHAR,
        last_name      VARCHAR,
        gender         VARCHAR,
        level          VARCHAR
    );
""")

song_table_create = ("""
    create table if not exists song (
        song_id        VARCHAR PRIMARY KEY,
        title          VARCHAR,
        artist_id      VARCHAR,
        year           INT,
        duration       DECIMAL
    );
""")

artist_table_create = ("""
    create table if not exists artist (
        artist_id      VARCHAR PRIMARY KEY,
        name           VARCHAR,
        location       VARCHAR,
        latitude      DECIMAL,
        longitude      DECIMAL
    );
""")

time_table_create = ("""
    create table if not exists time (
        start_time     TIMESTAMP PRIMARY KEY,
        hour           INT,
        day            INT,
        week           INT,
        month          INT,
        year           INT,
        weekday        INT
    );
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events 
    from {}
    iam_role {}
    json 's3://udacity-dend/log_json_path.json';
    ;
""").format(config.get('S3','LOG_DATA'), config.get('IAM_ROLE','ARN'))

staging_songs_copy = ("""
    copy staging_songs
    from {}
    iam_role {}
    format as json 'auto'
    ;
""").format(config.get('S3','SONG_DATA'), config.get('IAM_ROLE','ARN'))

# FINAL TABLES

songplay_table_insert = ("""
    insert into songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    select e.ts as start_time
        , cast(e.userId as int) as user_id
        , e.level
        , s.song_id
        , s.artist_id
        , e.sessionId as session_id
        , e.location
        , e.userAgent as user_agent
    from staging_events e
    join staging_songs s
        on e.song = s.title
        and e.artist = s.artist_name
    where e.page = 'NextSong'
    ;
""")

users_table_insert = ("""
    insert into users (user_id, first_name, last_name, gender, level)
    with recency_ranks as (
        select cast(userId as int) as user_id
            , firstName as first_name
            , lastName as last_name
            , gender
            , level
            , row_number() over (partition by userId order by ts desc) as recency_rank
        from staging_events
        where REGEXP_COUNT(userId, '^[0-9]+$') > 0 --only include records where there is a valid integer userId
    )
    select user_id
        , first_name
        , last_name
        , gender
        , level
    from recency_ranks
    where recency_rank = 1
    ;
""")

song_table_insert = ("""
    insert into song (song_id, title, artist_id, year, duration)
    select song_id
        , title
        , artist_id
        , year
        , duration
    from staging_songs
    ;
""")

artist_table_insert = ("""
    insert into artist (artist_id, name, location, latitude, longitude)
    select artist_id
        , artist_name as name
        , artist_location as location
        , artist_latitude as latitude
        , artist_longitude as longitude
    from staging_songs
    ;
""")

time_table_insert = ("""
    insert into time (start_time, hour, day, week, month, year, weekday)
    with ts as (
        select distinct timestamp 'epoch' + ts/1000 * interval '1 second' AS start_time 
        from staging_events
    )
    select start_time
        , extract(hour from start_time) as hour
        , extract(day from start_time) as day
        , extract(week from start_time) as week
        , extract(month from start_time) as month
        , extract(year from start_time) as year
        , extract(weekday from start_time) as weekday
    from ts
    ;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, users_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, users_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, users_table_insert, song_table_insert, artist_table_insert, time_table_insert]
