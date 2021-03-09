## Description
This is a SPARK Datalake ETL application which uses AWS EMR Cluster to process data.
The app processs and stores data used to analyze data regarding songs and user activity collected from 
a music streaming app. The data is sourced from AWS S3, processed and stored back to s3 as parquet files.


## Software required
* AWS EMR
* Release label:emr-5.20.0
* Hadoop distribution:Amazon
* Applications:Ganglia 3.7.2, Spark 2.4.0, Zeppelin 0.8.0

## Files/descriptions
* README.md - contains project/app details
* etl.py - python module to load staging tables from s3, then populate the 
  fact and dimension tables from the staging tables


## Setup/Execution Instructions
* put the etl.py file on the master node of EMR cluster
* also create a file name dl.cfg, place your AWS key details in it and store on master node
* run the following commands:
  - export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop/conf
  - /usr/local/bin/spark-submit --master yarn ./etl.py



## Output File Details 
The following parquet files are stored on s3
  
* Fact Table
  - songplays_table.parquet - records in log data associated with song plays, partioned by year, month 

* Dimension Tables
  - users_table.parquet - stores users in the app

  - song_table.parquet - stores song details, partitioned by year, artist

  - artist_table.parquet - stores artists info

  - times_table.parquet - records time dimension details, partitioned by year, month

## Creator

* Ed Flemister
    - [https://github.com/eflemist/sparkdatalake.git](https:////github.com/eflemist/sparkdatalake)
 
 
