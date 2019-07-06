Data Modeling with AWS


Sparkify, a startup, is interested in analyzing songs and user activity from a new streaming app. The analytics team is interested in understanding the songs users have been listening to in the past. Currently, they do not have a easy way to query their data. So, they need a database that can organize and store JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. I will create a workflow to optimize queries for their use case. 

Schema

Fact and dimension tables were defined for a star schema with an analytic focus.
Fact Table

songplays - records in log data associated with song plays i.e. records with page NextSong songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

Dimension Tables

users - users in the app user_id, first_name, last_name, gender, level

songs - songs in music database song_id, title, artist_id, year, duration

artists - artists in music database artist_id, name, location, lattitude, longitude

time - timestamps of records in songplays broken down into specific units start_time, hour, day, week, month, year, weekday


S3 Redshift Database

The AWS S3 Redshift Database was utilized as a Data Warehouse to make the relevant features avaliable in dimensional tables, so that analyst can easily query or request for the information they may need. If you run the **python sql_queries.py** file, it will create these tables stage, and store the necessary data.The **create_tables.py** is the Python implementation that will actually perform

ETL 

The ETL was done mainly in SQL but Python was used as a gateway for connecting to the databases. The transformation and data normalization was done using query statements. You can run this by using the **etl.py** script.
