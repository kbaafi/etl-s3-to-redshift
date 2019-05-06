import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
    
LOG_DATA                           = config.get("S3","LOG_DATA") 
LOG_JSONPATH                       = config.get("S3","LOG_JSONPATH")
SONG_DATA                          = config.get("S3", "SONG_DATA")
ARN                                = config.get("DWH","dwh_s3_iam_arn")
SCHEMA                             = config.get("DWH","dwh_schema")

# CREATE SCHEMA
dwh_schema_create                   = "create schema if not exists {}".format(SCHEMA)

# DROP TABLES

staging_events_table_drop            = "drop table if exists staging_events"
staging_songs_table_drop             = "drop table if exists staging_songs"
songplay_table_drop                  = "drop table if exists songplay"
user_table_drop                      = "drop table if exists users"
song_table_drop                      = "drop table if exists songs"
artist_table_drop                    = "drop table if exists artists"
time_table_drop                      = "drop table if exists time"

# CREATE TABLES

staging_events_table_create= ("""
    create table staging_events(
        artist                           varchar,
        auth                             varchar,
        firstName                        varchar,
        gender                           char(1),
        itemInSession                    int,
        lastName                         varchar,
        length                           numeric(12,4),
        level                            varchar(6),
        location                         varchar,
        method                           varchar,
        page                             varchar,
        registration                     timestamp,
        sessionId                        int,
        song                             varchar ,
        status                           varchar (10),
        ts                               timestamp,
        userAgent                        varchar,
        userId                           int
)    
""")

staging_songs_table_create = ("""
    create table staging_songs(
        num_songs                       int,
        artist_id                       varchar,
        artist_name                     varchar,
        artist_longitude                numeric(11,3),  
        artist_latitude                 numeric(11,3),
        artist_location                 varchar,
        song_id                         varchar,
        title                           varchar,
        duration                        numeric(12,6),
        year                            int
    )
""")

songplay_table_create = ("""
    create table songplay(
    start_time                         timestamp not null sortkey,
    user_id                            int null,
    level                              varchar,
    song_id                            varchar,
    artist_id                          varchar,
    session_id                         varchar null,
    location                           varchar null,
    user_agent                         varchar null
    ) diststyle even
""")

user_table_create = ("""
    create table users(
    user_id                            int not null             sortkey,
    first_name                         varchar not null,
    last_name                          varchar not null,
    gender                             char(1) not null 
    ) diststyle all;
""")

song_table_create = (""" 
    create table songs(
    song_id                           varchar not null          sortkey,
    song_title                        varchar not null,
    artist_id                         varchar not null,
    year                              int not null,
    duration                          numeric(12,6) not null
    ) diststyle all;
""")

artist_table_create = ("""
    create table artists(
    artist_id                        varchar not null           sortkey,
    artist_name                      varchar not null,
    artist_location                  varchar,
    artist_longitude                 numeric(11,8),
    artist_latitude                  numeric(11,8)
    ) diststyle all;
""")

time_table_create = ("""
    create table time(
    start_time                      timestamp not null          sortkey,
    hour                            int not null,
    day                             int not null,
    week                            int not null,
    month                           int not null,
    year                            int not null
    ) diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from '{}' iam_role '{}'
    json '{}' timeformat as 'epochmillisecs'
    """).format(LOG_DATA,ARN,LOG_JSONPATH)

staging_songs_copy = ("""
    copy staging_songs from '{}' 
    iam_role '{}' json 'auto'""").format(SONG_DATA,ARN)

# FINAL TABLES

songplay_table_insert = ("""
    insert into songplay
    (start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)    
        select
        e.ts as start_time,
        e.userId as user_id,
        e.level,
        s.song_id,
        s.artist_id,
        e.sessionId as session_id,
        e.location,
        e.userAgent as user_agent
        from
        staging_events e left outer join
        staging_songs s on
        (
            s.artist_name = e.artist and
            s.title       = e.song and 
            s.duration    = e.length
        )
""")

user_table_insert = ("""
    insert into users
    (user_id,first_name,last_name,gender)
        select distinct userId,firstName,lastName,gender
        from staging_events
        where userId is not null
""")

song_table_insert = ("""
    insert into songs
    (song_id,song_title,artist_id,year,duration)
        select distinct song_id,title, artist_id,year,duration
        from staging_songs
        where song_id is not null
""")

artist_table_insert = ("""
    insert into artists
    (artist_id,artist_name,artist_location,artist_latitude, artist_longitude)
        select distinct artist_id, artist_name,artist_location, 
            artist_latitude, artist_longitude
        from staging_songs
        where artist_id is not null        
""")

time_table_insert = ("""
insert into time
(day, hour, month, start_time, week, year)
select distinct 	
    extract (day from ts) as day,
    extract (hour from ts) as hour,
    extract (month from ts) as month,
    ts as start_time,
    extract (week from ts) as week,
    extract (year from ts) as year
  from staging_events
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
drop_staging_queries = [staging_events_table_drop, staging_songs_table_drop]