# Data-Lake-Project

## Project Description

This project has to purpose to support a startup called Sparkify, with time it has grown their user base and song database and want to move their processes and data onto the cloud.
Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.
As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

### Scripts/files
* `etl.py` - Uses spark to process data from one S3 bucket to another S3 bucket
* `dl.cfg` - Config file with the necessary info to access AWS resources

## DB Design

The schema design is based on the concept of a star schema. It is desgined to analyse the data regarding song playing.
This type of modeling takes advantage of denormalization and fast aggregation.
It will help Sparkify answer their questions with fast and reliable data.

## Tables 

### Fact Table

1. **songplays** - records in log data associated with song plays
   * songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### Dimension Tables

2. **users** - users in the app
   * user_id, first_name, last_name, gender, level

3. **songs** - songs in music database
   * song_id, title, artist_id, year, duration

4. **artists** - artists in music database
   * artist_id, name, location, latitude, longitude

5. **time** - timestamps of records in songplays broken down into specific units
   * start_time, hour, day, week, month, year, weekday



## How to run locally

1. Clone repo and change diretory

```bash
git git@github.com:Alkarya/Data-Lake-Project.git

cd data-lake-project
```

2. Create Python venv

On a new terminal run:
```bash
python3 -m venv python-venv            
source python-venv/bin/activate 
```

3. Install requirements

```bash
pip install -r requirements.txt
```

4. Create in AWS console an S3 bucket to for the output to be stored

5. Add necessary info to the dwh.cfg file

```cfg
[AWS]
AWS_ACCESS_KEY_ID='insert_access_key_id_here'
AWS_SECRET_ACCESS_KEY='insert_secret_access_key_here'
```

6. Run python scripts

```bash
cd scripts
python -m etl
```

6. Closing and cleaning the local environment

```bash
deactivate
rm -r python-venv
```

7. Delete S3 bucket

## References

* [PySpark SQL Types (DataType) with Examples](https://sparkbyexamples.com/pyspark/pyspark-sql-types-datatype-with-examples/) 
* [Python timestamp to datetime and vice-versa](https://www.programiz.com/python-programming/datetime/timestamp-datetime) 
* [java.lang.NumberFormatException: in Pyspark when writing to S3](https://stackoverflow.com/questions/71097630/java-lang-numberformatexception-in-pyspark-when-writing-to-s3)