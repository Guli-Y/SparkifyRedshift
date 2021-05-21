Sparkify is a startup company who provides music streaming services. They have JSON metadata on the songs in their app and user activity data in Amazon S3. The analytics team in Sparkify is interested in understanding what songs users are listening to. 


## Purpose

To build a ETL pipeline for extracting data from s3, stage them on Redshift and transform them into dimensional tables for song play analysis.


## Star schema design


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


## Usage 

1. Create a Redshift cluster and update config.ini with your own credentials

2. Create the tables in your Redshift database by running following command in your terminal

  ```python creat_tables.py```

3. Stage, tranform and load the data to your Redshift database by running following command

  ```python etl.py```

4. Check whether the tables are loaded correctly by running following command

  ```python test_tables.py```


## Credits
The codes are written by me and the sparkify data belongs to Udacity.
