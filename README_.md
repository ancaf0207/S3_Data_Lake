

## **Context and purpose:**


*Introduction
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.
As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.
You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.*



## **Design and execution:**

The data available for Sparkify was stored in .json format in 2 S3 repositories:

    *Song Dataset:* metadata about a song and the artist of that song.
    *Log Dataset:* log files for events representing a song play.


For the Sparkify project, their data was re-structured in a star schema:


**Fact Table**
This contains the actual metric of the business: the event of playing a song.

songplays - records in log data associated with song plays i.e. records with page NextSong
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

The granularity (uniqueness) of each song play is not given by any of the fields in the dataset so, for primary key it was used an auto numbering field: sp_id.
Based on the introduction, the 2 main focal points are the user and the songs. Therefore, the songplay data was distributed by sp_user_id, and the sorting was done with a compound sortkey containing the user and song id. 
Also, these fields will be used intensively for the Joins and filters, so having them as  distr and/or sort keys  it will boost the performance of the queries.

**Dimension Tables**
These are detailed description of the 'entities' that participate in the event of playing a song (artist, song, user)

users - users in the app: user_id, first_name, last_name, gender, level
song - songs in music database: song_id, title, artist_id, year, duration
artist - artists in music database: artist_id, name, location, latitude, longitude
time - timestamps of records in songplays broken down into specific units: start_time, hour, day, week, month, year, weekday


The artist and song tables were created extracting data from the Song dataset.
The source data (the JSON files) were first loaded from S3 and, after removing the duplicates, the parquet files were written to the new S3 bucket – for songs and then for artists.
For efficiency, the parquet songs were partitioned by year and artist_is.

The users and time tables were created extracting data from the Log dataset. For the time table, extraction of year, month, etc. from the timestamp was also performed.
The source data (the JSON files) were first loaded from S3 and, after removing the duplicates, the parquet files were written to the new S3 bucket.
For efficiency, the parquet time data  was partitioned by year and month.

The fact table (songplays) had most of the data pulled from the Log dataset, except for the song id and artist id which were extracted from the songs dataset.
In order to merge the two data sets, the data was loaded into 2 dataframes and then joined on the artist name. The parquet files were also partitioned by the year and month (like the time table) in order to optimize query time.

# **Project files and structure**

The project consists of the following **files**:

    
    A. etl.py

This is the working notebook for the design of the ETL process.
It includes the step by step process of extracting the data from the JSON files, transforming it and loading it to a new S3 bucket as parquet files. 

    B. dl.cfg

This is the settings file in which you need to fill in your cluster credentials. The data here will be used by the etl script to connect to the Spark Cluster.

    and C. the readme.md.
    
    
**Running the scripts:**
Pre requisites: You need to have a your S3 destination bucket created and added in the script; you need to have AWS IAM keys added in the dl.cfg. 


    1. Open a new terminal;
    2. Write in the console: python3 etl.py and press Enter
    4. Check your S3 bucket – you should have your parquet data available.
