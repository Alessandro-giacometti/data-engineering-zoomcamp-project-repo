# data-engineering-zoomcamp-project-repo
My repository for the Data Engineering ZoomCamp course.
Link to official course repository by DataTalksClub: https://github.com/DataTalksClub/data-engineering-zoomcamp

## WEEK 1 - DOCKER & SQL
### DOCKER RUN POSTGRES CODE
docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v dtc-postgres_volume_local:/var/lib/postgresql/data \
  -p 5432:5432 \
  postgres:13

### LOGIN INTO DB WITH PGCLI
 pgcli -h localhost -p 5432 -u root -d ny_taxi


### DOCKER RUN POSTGRES - WITH NETWORK-NAME FOR PGADMIN
docker network create pg-network

Check if network is created:
docker network ls

Run new docker container
docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v dtc-postgres_volume_local:/var/lib/postgresql/data \
  -p 5432:5432 \
  --network=pg-network \
  --name pg-database \
  postgres:13



### PGADMIN DOCKER CONTAINER
docker run -it \
    -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
    -e PGADMIN_DEFAULT_PASSWORD="root" \
    -p 8080:80 \
    --network=pg-network \
    --name pgadmin \
    dpage/pgadmin4


### CONNECTION TO POSTGRES DB TROUGHT PGADMIN WEB INTERFACE
http://localhost:8080/browser/
user: admin@admin.com
pw: root

### CREATE (REGISTER) NEW SERVER
Servers > Register new server

Name: Local docker
Host name/address: pg-database
Port: 5432
User: root
Pw: root


## DOCKERIZE THE INGESTION PIPELINE SCRIPT
Convert the Jupyter notebeook to a Python script from command line:
jupyter nbconvert --to=script upload-data.ipynb

Then clean the script and rename to "ingest-data.py"
Then add argparse library and add a snippet to parse the arguments from the command line, and create a main function.

### Test the ingestion script
Drop the table from pgadmin, to clean the db.
Then test the script by running it throu command line:

python ingest_data.py \
  --user=root \
  --password=root \
  --host=localhost \
  --port=5432 \
  --db=ny_taxi \
  --table_name=yellow_taxi_trips \
  --url="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet"

### Actually dockerize the script
docker build -t taxi_ingest:v001 .

### Run the ingestion script

Remember to set the network and to update the the host

docker run -it \
  --network=pg-network \
  taxi_ingest:v001 \
  --user=root \
  --password=root \
  --host=pg-database \
  --port=5432 \
  --db=ny_taxi \
  --table_name=yellow_taxi_trips \
  --url="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet"


## DOCKER COMPOSE TO RUN SIMOULTANESLY POSTGRES AND PGADMIN
For Linux, it's necessary to download separately Docker Compose.
Create the "docker-compose.yaml" file with the configuration settings.

In addition to course video, to persist the pgadmin configuration, add this volume to pgadmin configuration:
"pgadmin_conn_data:/var/lib/pgadmin:rw"

and this code at the end of the configuration file:
"./pgadmin_conn_data:/var/lib/pgadmin:rw"

Finally, to run the docker compose container use this command:
docker-compose up

To stop the conatiner use the command:
docker-compose down

If you want to keep the terminal, use the "detached mode" with this code:
docker-compose up -d

## RE-INGEST DATA TO POSTGRES DB IN THE DOCKER-COMPOSE CONAINTER
Need to specify the default network create by docker compose.
Remember that in docker compose, the host name is equal to the service name (pgdatabase)
docker run -it \
  --network=docker_sql_default \
  taxi_ingest:v001 \
  --user=root \
  --password=root \
  --host=pgdatabase \
  --port=5432 \
  --db=ny_taxi \
  --table_name=yellow_taxi_trips \
  --url="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet"



# HOMEWORK WEEK 1

## Module 1 Homework: Docker & SQL
### Question 1
cd "C:\Users\Aless\Documents\GIT\data-engineering-zoomcamp-project-repo\01_docker_sql\homework\1"

Answer: 24.3.1

### Question 2
Answer: db:5432

### Question 3
SELECT 
	COUNT(*)
FROM public.green_taxi_trips_oct_19
WHERE CAST(lpep_pickup_datetime AS DATE) >= '2019-10-01'
	AND CAST(lpep_dropoff_datetime AS DATE) < '2019-11-01'
	AND trip_distance <= 1;

1) Up to 1 mile: 104802
2) In between 1 (exclusive) and 3 miles (inclusive): 198924
3) In between 3 (exclusive) and 7 miles (inclusive): 109603
4) In between 7 (exclusive) and 10 miles (inclusive): 27678
5) Over 10 miles: 35189

Answer: 104,802; 198,924; 109,603; 27,678; 35,189

### Question 4
SELECT 
	CAST(lpep_pickup_datetime AS DATE) as trip_day,
	MAX(trip_distance) as max_trip_distance
FROM public.green_taxi_trips_oct_19
WHERE CAST(lpep_pickup_datetime AS DATE) >= '2019-10-01'
	AND CAST(lpep_pickup_datetime AS DATE) < '2019-11-01'
GROUP BY CAST(lpep_pickup_datetime AS DATE)
ORDER BY MAX(trip_distance) DESC;

Max trip distance: 515.89
Answer: 2019-10-31

### Question 5
SELECT 
	t."PULocationID",
	z."Zone",
	SUM(total_amount) as location_total_amount
FROM public.green_taxi_trips_oct_19 AS t
LEFT JOIN public.zones AS z
ON t."PULocationID" = z."LocationID"
WHERE CAST(lpep_pickup_datetime AS DATE) = '2019-10-18'
GROUP BY t."PULocationID", z."Zone"
HAVING SUM(total_amount) > 13000
ORDER BY SUM(total_amount) DESC

1) East Harlem North
2) East Harlem South
3) Morningside Heights

Answer: East Harlem North, East Harlem South, Morningside Heights

### Question 6
SELECT 
	t."DOLocationID",
	z."Zone",
	MAX(tip_amount)
FROM public.green_taxi_trips_oct_19 AS t
LEFT JOIN public.zones AS z
ON t."DOLocationID" = z."LocationID"
WHERE CAST(lpep_pickup_datetime AS DATE) >= '2019-10-01'
	AND CAST(lpep_pickup_datetime AS DATE) < '2019-11-01'
	AND t."PULocationID" IN (SELECT "LocationID" FROM public.zones WHERE "Zone" = 'East Harlem North')
GROUP BY t."DOLocationID", z."Zone"
ORDER BY MAX(tip_amount) DESC;

Max tip: 87.3
Anwser: JFK Airport

## WEEK 2 - KESTRA
I will run Kestra in Docker and use it orchestrate a pipeline to extract data and put in a Postgres database. To access the database I will use Pgadmin.
I will use Docker Compose to run all the containers.

### SETUP DOCKER
I created a docker-compose.yaml file to setup the configuration and run Kestra, Postgres and PGadmin.

To run the container:
docker-compose up -d

### Local DB: Load Taxi Data to Postgres

### Local DB: Learn Scheduling and Backfills

### Local DB: Orchestrate dbt Models


# HOMEWORK WEEK 2

## Module 2 Homework: Kestra Orchestration
### Question 1
Run the "homework_extract_taxi_csv" flow selecting type "yellow", year "2020" and month "12", which extracts the "yellow_tripdata_2020-12.csv" file.
From "Executions > Output" is possible to view the ouput file size.

Answer: 128.3 MB

### Question 2
Run the "homework_extract_taxi_csv" flow selecting type "green", year "2020" and month "04" and see the rendered value of the value "file" during the execution.

Answer: green_tripdata_2020-04.csv

### Question 3
SELECT COUNT(*)
FROM public.yellow_tripdata
WHERE filename LIKE 'yellow_tripdata_2020_%'

Answer: 24648499

### Question 4
SELECT COUNT(*)
FROM public.green_tripdata
WHERE filename LIKE 'green_tripdata_2020_%'

Answer: 1734051

### Question 5
SELECT COUNT(*)
FROM public.yellow_tripdata
WHERE filename = 'yellow_tripdata_2021-03.csv'

Answer: 1925152

### Question 6
Documentation for changing timezone in the Schedule trigger configuration: https://kestra.io/plugins/core/triggers/io.kestra.plugin.core.trigger.schedule

Answer: Add a timezone property set to America/New_York in the Schedule trigger configuration



# HOMEWORK WEEK 3

## Module 3 Homework: Data Warehouse and BigQuery

Uploaded the Yellow Taxi data (parquet files) to GCS bucket with "load_yellow_taxi_data.py" script.

After that, created:
1) An external table from bucket files:

CREATE OR REPLACE EXTERNAL TABLE `de-zoomcamp-ag15.ny_taxi.yellow_taxi_external`
  OPTIONS (
    format ='PARQUET',
    uris = ['gs://dezoomcamp_ag15_hw3_2025/yellow_tripdata_2024-*.parquet']
    );

 2) Materialized/regular non-partitioned table in BigQuery from external file:

CREATE OR REPLACE TABLE `de-zoomcamp-ag15.ny_taxi.yellow_taxi_non_partitioned` AS
SELECT * FROM `de-zoomcamp-ag15.ny_taxi.yellow_taxi_external`;


### Question 1
What is count of records for the 2024 Yellow Taxi Data?

SELECT
  COUNT(*)
FROM `de-zoomcamp-ag15.ny_taxi.yellow_taxi_external`

Answer: 20332093

### Question 2
Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.
What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

SELECT
  COUNT(DISTINCT PULocationID)
FROM `de-zoomcamp-ag15.ny_taxi.yellow_taxi_external`;
Estimated processed data: 0B

SELECT
  COUNT(DISTINCT PULocationID)
FROM `de-zoomcamp-ag15.ny_taxi.yellow_taxi_external`;
Estimated pocessed data: 155.12 MB 

Answer: 0 MB for the External Table and 155.12 MB for the Materialized Table

### Question 3
Write a query to retrieve the PULocationID from the table (not the external table) in BigQuery. Now write a query to retrieve the PULocationID and DOLocationID on the same table. Why are the estimated number of Bytes different?

SELECT
  PULocationID
FROM `de-zoomcamp-ag15.ny_taxi.yellow_taxi_non_partitioned`;
This query will process 155.12 MB when run.
SELECT
  PULocationID,
  DOLocationID
FROM `de-zoomcamp-ag15.ny_taxi.yellow_taxi_non_partitioned`;
This query will process 310.24 MB when run.


Answer: BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.

### Question 4
How many records have a fare_amount of 0?

SELECT
  COUNT(*)
FROM `de-zoomcamp-ag15.ny_taxi.yellow_taxi_external`
WHERE fare_amount = 0;

Anwser: 8333

### Question 5
What is the best strategy to make an optimized table in Big Query if your query will always filter based on tpep_dropoff_datetime and order the results by VendorID (Create a new table with this strategy)

CREATE OR REPLACE TABLE `de-zoomcamp-ag15.ny_taxi.yellow_taxi_partitioned_clustered`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM `de-zoomcamp-ag15.ny_taxi.yellow_taxi_non_partitioned`;

Answer: Partition by tpep_dropoff_datetime and Cluster on VendorID

### Question 6
Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive)

Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 5 and note the estimated bytes processed. What are these values?

- Materialized non-partitioned table:
SELECT 
  DISTINCT VendorID
FROM `de-zoomcamp-ag15.ny_taxi.yellow_taxi_non_partitioned`
WHERE tpep_dropoff_datetime  BETWEEN '2024-03-01' AND '2024-03-15';
This query will process 310.24 MB when run.

- Materialized partitioned table:
 SELECT 
  DISTINCT VendorID
FROM `de-zoomcamp-ag15.ny_taxi.yellow_taxi_partitioned_clustered`
WHERE tpep_dropoff_datetime  BETWEEN '2024-03-01' AND '2024-03-15'
This query will process 26.84 MB when run.

Answer: 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table

### Question 7
Where is the data stored in the External Table you created?

Answer: GCP Bucket

### Question 8
It is best practice in Big Query to always cluster your data:

Answer: False


### Question 9
No Points: Write a SELECT count(*) query FROM the materialized table you created. How many bytes does it estimate will be read? Why?

SELECT 
  COUNT(*)
FROM `de-zoomcamp-ag15.ny_taxi.yellow_taxi_non_partitioned`;
This query will process 0 B when run.

BigQuery pre-compute and stores some metadata of the materialized table, such as the total row count.
