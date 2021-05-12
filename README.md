Sparkify is a startup company who provides music streaming services. They have JSON metadata on the songs in their app and user activity data in Amazon S3. The analytics team in Sparkify is interested in understanding what songs users are listening to. 


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


## how to use it 

STEP1: create a Redshift cluster and replace the params values in config.ini 

STEP2: to create the needed tables in your Redshift database: python creat_tables.py 

STEP3: to stage, tranform and load the data to your Redshift database: python etl.py 

STEP4: to test whether the tables are loaded correctly: python test_tables.py


## note
The codes are written by me and the sparkify data belongs to Udacity.