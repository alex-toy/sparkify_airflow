# Data Engineering project 5 - Data Warehouse ETL pipeline automation and monitoring with Airflow

By Alessio Rea

==============================

You need to have Python 3.8.5 installed for this project

# General explanation

## 1. Purpose of the project

The purpose of the project is to introduce automation and monitoring to a data warehouse ETL pipelines using **Apache Airflow**. **Airflow** allows to create high grade data pipelines that are dynamic and built from reusable tasks, that can be monitored, and allow easy backfills. It also allows to monitor data quality which plays a big part when analyses are executed on top the data warehouse, in that it allows to run tests against our datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in a cloud provided data warehouse (Amazon Redshift). The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.


## 2. Database schema design and ETL pipeline

The two datasets that reside in S3. Here are the S3 links for each:

    - Song data: s3://udacity-dend/song_data
    - Log data: s3://udacity-dend/log_data


- Song Dataset
    
    Here are filepaths to two files that could be found in such a dataset :

    ```
    song_data/A/B/C/TRABCEI128F424C983.json
    song_data/A/A/B/TRAABJL12903CDCF1A.json
    ```

    Here is an example of what a single song file may looks like :

    ```
    {"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
    ```

    Those files contain the following features : 'artist_id', 'artist_latitude', 'artist_location', 'artist_longitude', 'artist_name', 'duration', 'num_songs', 'song_id', 'title', 'year'

- Log Dataset
    
    Here are filepaths to two files that could be found in such a dataset :

    ```
    log_data/2018/11/2018-11-12-events.json
    log_data/2018/11/2018-11-13-events.json
    ```
    
    Those files contain the following features : 'artist', 'auth', 'firstName', 'gender', 'itemInSession', 'lastName',
       'length', 'level', 'location', 'method', 'page', 'registration',
       'sessionId', 'song', 'status', 'ts', 'userAgent', 'userId'


Here is how the data is modelled according to a star schema :

- Fact table : table songplays containing the following features : songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

- Dimension tables : 

    - users - users in the app. Features : user_id, first_name, last_name, gender, level
    - songs - songs in music database. Features : song_id, title, artist_id, year, duration
    - artists - artists in music database. Features : artist_id, name, location, latitude, longitude
    - time - timestamps of records in songplays broken down into specific units. Features : start_time, hour, day, week, month, year, weekday


## 3. Example queries and results for song play analysis

Once the data has been ETLed, you are free to take full benefit from the power of star modelling and make business driven queries like :

    - Which song has been played by user 'Lily' on a paid level?
    - When did user 'Lily' play song from artist 'Elena'?



# Project Organization 
----------------------

    ????????? dags
    ??????? ????????? create_tables.sql     <- Create_tables in Redshift.
    ??????? ????????? global_dag.py         <- Creates DAG and runs all tasks in sequence.
    ????????? plugins
    ??????? ????????? helpers               
    ???   ??????? ????????? sql_queries.py    <- For ETL purpose. 
    ??????? ????????? operators 
    ???   ??????? ????????? data_quality.py   <- Run data quality checks.  
    ???   ??????? ????????? load_dimension.py <- Populate dimension tables.  
    ???   ??????? ????????? load_fact.py      <- Populate fact tables.               
    ???   ??????? ????????? stage_redshift.py <- Create staging tables in Redshift based on S3 data. 
    ????????? utils
    ??????? ????????? dwh.cfg               <- Config file containing credentials. Hide it!!
    ??????? ????????? Iac_1.py              <- Creates new iam role, attaches policy AmazonS3ReadOnlyAccess to it and finally creates new cluster programmatically.
    ??????? ????????? Iac_2.py              <- Open an incoming TCP port to access the cluster endpoint.
    ??????? ????????? release_resources.py  <- Automatically release all resources created on Redshift.
    ??????? ????????? settings.py           <- Useful functions for project.  
    ????????? init.sh                   <- useful command line instructions.
    ????????? requirements.txt          <- Necessary packages for local use.
    ????????? README.md                 <- The top-level README for users and developers using this project.


# Getting started

## 1. Clone this repository

```
$ git clone <this_project>
$ cd <this_project>
```

## 2. Install requirements

I suggest you create a python virtual environment for this project : <https://docs.python.org/3/tutorial/venv.html>


```
$ pip install -r requirements.txt
```

--------


## 2. Configuration of project

You need to have an AWS account to run the complete analysis. You also need to create a user that has AmazonRedshiftFullAccess as well as AmazonS3ReadOnlyAccess policies. Make sure to keep its KEY and SECRET credentials in a safe place.

1. Copy the *dwh.cfg* into a safe place.
2. Fill in all fields except *LOG_DATA*, *LOG_JSONPATH*, *SONG_DATA* which are already filled and *DWH_ENDPOINT*, *DWH_ROLE_ARN* which will be automatically filled for you. 
3. In file *settings.py*, give the path to *dwh.cfg* to variable *config_file*.
4. Run *IaC_1.py* and wait untill you see the cluster available in your console.
4. Run *IaC_2.py*.
5. At the end, don't forget to *release_resources.py* !!!!



## 3. Run project

You need to have Airflow installed to run the project. Find global_dag and run it. As it runs, you can go into the query section of your Redshift console and see the creation and populating of the tables.


--------




