[star_schema]:/images/starschema.png "Million Songs DW Schema Design"

# Data Ingest from AWS S3 to Redshift

## Dependencies

- configparser

- psycopg2

- boto3

- json

## Running the ETL Pipeline

Running the pipeline consists of two processes:

1. Spin up the infrastructure on AWS using the included _Redshift Cluster Generator_
    To do that run ```python deploy_redshift_iac.py``` for more about this see [Redshift-Cluster-Generator](https://github.com/kbaafi/Redshift-Cluster-Generator)

2. Run ```python etl.py``` to load the data for further analysis

## Data Sources

The input data consists of two datasets currently stored on AWS S3:

1. A subset of real song data from the Million Song Dataset: Each file is in JSON format and contains metadata about a song and the artist of that song. The metadata consists of fields such as song ID, artist ID, artist name, song title, artist location, etc. A sample song record is shown below:

    ```javascript
       {"num_songs": 1, "artist_id": "ARD7TVE1187B99BFB1", "artist_latitude": null, 
    "artist_longitude": null, "artist_location": "California - LA", "artist_name": "Casual", 
        "song_id": "SOMZWCG12A8C13C480", "title": "I Didn't Mean To", "duration": 218.93179, "year": 0} 
    ```

2. Logs of user activity on a purported music streaming application. The data is actually generated using an event generatory. This data captures user activity on the app and stores metadata such as artist name, authentication status, first name,length ,time , ect. A sample of the log data is shown below:

    ```javascript
        {"artist":"N.E.R.D. FEATURING MALICE","auth":"Logged In","firstName":"Jayden","gender":"M","itemInSession":0,"lastName":"Fox","length":288.9922,"level":"free",
    "location":"New Orleans-Metairie, LA","method":"PUT","page":"NextSong",
        "registration":1541033612796.0,"sessionId":184,"song":"Am I High (Feat. Malice)",
            "status":200,"ts":1541121934796,"userAgent":"\"Mozilla\/5.0 (Windows NT 6.3; WOW64) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"","userId":"101"}
    ```

## Database Design

### Fact Table(s)

**Songplays:** This table tracks the songs played by users on the platform. It tracks the following metadata:

```javascript
{song_play_id, session_id, user_id,start_time,artist_id,location,song_id, agent}
```

### Dimension Table(s)

**Users:** Maintains unique user data on users of the platform

**Artists:** Maintains unique artist metadata

**Songs:** Maintains unique song metadata

**Time:** Allows tracking of time at which user interactions with the platform occurs

### Schema Design

The schema design is shown below:

![star_schema]

### Design Considerations

#### Distribution Styles

The fact table **songplays** is distributed evenly across all Redshift slices _(diststyle even)_. All other tables are fully distributed across all slices _(diststyle all)_. The dimension tables are small enough to warrant full replication on all slices, therefore allowing shuffle free joins and quick query responses

#### Sort Keys

All tables are presorted before insert on sort keys. The sortkeys used for each table are shown below. The DDLs for the tables are defined in ```sql_queries.py```. Do have a look

- **songplays:** _start_time_

- **users:** _user_id_

- **artists:** _artitst_id_

- **songs:** _song_id_

- **time:** _start_time_

## Results

The results of this ETL can be confirmed by running the notebook test_etl.ipynb