# ETL data pipelines with airflow.
To bring in more automation and monitoring of the data warehouses and ETL pipelines 
created in the previous projects [etl-pipeline](https://github.com/theKoladeAkande/etl-pipeline) and
[etl-datawharehouse](https://github.com/theKoladeAkande/etl-datawharehouse) 
Apache Airflow is introduced into the project system.

## Context Problem
To create  data pipelines with reusable tasks, which can be monitored and
permit backfilling. Also,data quality plays a big part when analysis are executed on top the data warehouse and want to run tests against their datasets for 
data validation after the ETL steps have been executed, to catch any conflict in the datasets.
The source data resides in S3 and needs to be processed in the start-up's data warehouse in Amazon Redshift. 

## Solution
### DAGs (Directed Acyclic Graphs)
Dags are used in modeling and scheduling tasks and their depedencies,
the dag for this project (sparkify_etl_dag) is modeled programatically with python using airflow.
The image below is a directed acyclic graph for this project showing the various tasks and their dependencies
![](https://github.com/theKoladeAkande/etlpipline-airflow/blob/master/airflow%20diagram.png)

A tree view.
![](https://github.com/theKoladeAkande/etlpipline-airflow/blob/master/tree%20view.png)


### TASKS AND OPERATORS
tasks areoperations and activities to be peformed,An operator represents a single, ideally idempotent, task
for this project here are the tasks and operators defined in airflow for building and scheduling the ETL pipeline.

#### create_tables
Connects to redshifts and creates the facts and dimension tables required.
**Create Table Operator** reads and process a .sql file to filter out the queries for table creation,
connects to  redshift and creates the tables.

#### staging_data
Connects to redshifts and stages the data from s3 in redshifts.
**Stage table in redshift operator** connects to redahifts and execute the sql query to copy files from s3 to redshift
this operator also provides contexts to allow backfilling, execution by timestamps.

##### load_fact_table
Transforms and loads the data staged in redshift into the fact table.

##### load_dimension_table
Transforms and loads the data staged in redhsift into dimensuon table 

#### Run_data_quality_checks
Perform data validation after the etl process has been completed to ensure that tables are actually loaded and raises 
an error if the test isn't passed

### Configurations:
The DAG could be configured:
* Scheduling to runs within a specfied interval. 
* Retry within a specified duration,also for a particular number of times.
* Send email  during retry.

