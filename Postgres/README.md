# **Project: Data Modeling with Postgres**

# Introduction
Sparkify a music streaming startup wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently the data resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

In this project a star schema for database schema, designed for optimizing queries on song play analysis. In this project we will develop ETL pipeline for this analysis.

# Database schema design and ETL pipeline.

## From this representation we can already start to see the makings of a "STAR". We have one fact table (the center of the star) and 4  dimension tables that are coming from it.

### Fact Table
`Table Name: songplays, Description: records in log data associated with song plays
column: songplay_id
column: start_time
column: user_id
column: level
column: song_id
column: artist_id
column: session_id
column: location
column: user_agent`

### Dimension Tables

`Table Name: users, Description: users in the app
column: user_id
column: first_name
column: last_name
column: gender
column: level`

`Table Name: songs, Description: songs in music database
column: song_id
column: title
column: artist_id
column: year
column: duration`

`Table Name: artists, Description: artists in music database
column: artist_id
column: name
column: location
column: latitude 
column: longitude`

`Table Name: time, Description: timestamps of records in songplays broken down into specific units
column: start_time
column: hour
column: day
column: week
column: month
column: year
column: weekday`

## Star schema makes easy for business sense it will reduce the need for joining and easy to understand.

-Denormalized

-Fast aggregation

-Simplified queries

ETL Pipeline developed reads data from json file transfroms the data and stages the data in stagging table `copy` and loads the data in destination tables

Storing in staging table lets quality check and gives oppurtunity to load mutliple records at once as well as use `upsert` to set rules for `conflict` resolution. 

ETL pipeline developed here will update `user`, `song` and `artist` dimension tables with latest records as there can be. In log files there can be case when at same time stamp there are multiple records etl pipeline do not update `time` table. 


## Instruction to run the script or Notebook

- run python script `create_tables.py` for generating schema and run `etl.py` to execute ETL

- Run all cells of notebook `Demo.ipynb`