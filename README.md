Sparkify is a startup company who provides music streaming services. They have JSON metadata on the songs in their app and user activity data in Amazon S3. The analytics team in Sparkify is interested in understanding what songs users are listening to. In this repo, I will build a ETL pipeline to extract their data from s3, stage the data on Redshift and load the data into a set of dimensional tables for Sparkify analytics team. 

## purpose

To build a ETL pipeline for extracting data from s3, stage them on Redshift and transform them into dimensional tables for song play analysis.


## star schema design


**Fact Table**

songplays
- songplay_id (primary key), start_time (foreign key), user_id (foreign key), level, song_id (foreign key), artist_id (foreign key), session_id, location, user_agent

**Dimension Tables**

users

- user_id (primary key), first_name, last_name, gender, level

songs

- song_id (primary key), title, artist_id (foreign key), year, duration

artists

- artist_id (primary key), name, location, latitude, longitude

time

- start_time (primary key), hour, day, week, month, year, weekday


# how 

STEP1: create a Redshift cluster and replace the params values in config.ini 

STEP2: run creat_tables.py to create the needed tables in your Redshift database

STEP3: run etl.py to stage the data, tranform the data, and load the data on your Redshift database

STEP4: run test_tables.py to test whether the tables are loaded correctly


# note
The codes are written by me and the sparkify data belongs to Udacity.