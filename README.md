# Readme.md for Redshift ETL pipeline for Sparkify database

## Purpose:
The purpose of this database is to provide datasets that enable answering questions about user behavior in the Sparkify app (especially user song 
listening behavior). To do so we ingest the app logs of user behavior and song metadata into a Redshift database, 'dwh', from where we can run
queries to answer such questions.

## The files in the repository:
* create_tables.py: drops and creates the tables. Run this file to reset your (empty) tables before each time you run your ETL scripts. This db does a full drop and re-write of tables each refresh (could convert to incremental inserts for efficiency/cost savings if desired)
* etl.py: loads S3 files (song_data and log_data) into staging tables and then loads transformed data into final tables
* sql_queries.py: contains all of the sql queries, and is imported into the files above (those files run these queries)
* dwh.cfg: this file is not included in the repo, but is needed to run this ETL. Create a 'dwh.cfg' file with your own AWS credentials to be able to run this ETL flow 
* test.py: manually running this prints some metadata (which tables are in the db) + sample data from each table to enable you to check that db looks as expected after the flow runs
* sparkify_data_erd.png: this is an image file that provides an easy-to-understand overview of the tables in the database. This image gets re-created each time the db is populated (via the etl.py script), so changes to the db will be reflected in this image
* Note: this pipeline populates the DWH from locally stored source song_data and log_data files, so you must also have those (not included here) to run this ETL and populate your own DWH


## How to run the Python scripts to create and populate the Redshift database:
The full ETL flow only consists of three steps:
1. Create a 'dwh.cfg' file that contains your AWS configuration details (you must also have source data files for song_data and log_data stored in S3)
2. Run create_tables.py to drop and create all tables
3. Run etl.py to populate tables with data from the song_data and log_data files and create the sparkify_data_erd.png image to provide an overview of the database design
4. (OPTIONAL) Run test.py to print to console the names of all tables created and a sample of data in each

The other files are all either supporting files (e.g. sql_queries.py stores queries that the other .py scripts use) or are for testing (test.py useful for testing new/changed functionality before implementing in the production files)

NOTE: for the creation of the ER diagram to work, you must **uninstall** the 'sqlalchemy' package (if already installed) and instead install 'sqlalchemy-redshift'. Otherwise you will get an `AttributeError: type object 'URL' has no attribute 'create'` error.

## Packages needed to run ETL flow:
```
pip uninstall sqlalchemy
pip install sqlalchemy-redshift
pip install redshift_connector
pip install sqlalchemy_schemadisplay
pip install psycopg2
```

## Database schema design and ETL pipeline:
We've designed the database to have one fact table to store user song listening activity and a handful of dimension tables to store relevant entities. The fact table, 'songplays', is relatively normalized, so users will need to join to relevant tables to pull in most song/artist/user details.

Fact Table(s):
* songplays: stores the following for each user listening activity: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

Dim Tables:
* songs: user_id, first_name, last_name, gender, level
* artists: artist_id, name, location, latitude, longitude
* users: user_id, first_name, last_name, gender, level
* time: start_time, hour, day, week, month, year, weekday

![alt text](https://github.com/mimoyer21/udacity-sparkifydb-redshift/blob/main/sparkify_data_erd.png?raw=true) 

## Example queries and results for song play analysis:
(to be filled in with more examples later if desired)

Total number of song plays by all users during a given time period (in this case, 2018):
```sql
select count(*) 
from songplay sp 
join time t 
    on sp.start_time = t.start_time 
where t.year = 2018;
```
