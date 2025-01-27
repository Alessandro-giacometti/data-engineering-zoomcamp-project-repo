# data-engineering-zoomcamp-project-repo
My repository for the Data Engineering ZoomCamp course.

## DOCKER + POSTGRES
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



# HOMEWORK

## Module 1 Homework: Docker & SQL
### Question 1
cd "C:\Users\Aless\Documents\GIT\data-engineering-zoomcamp-project-repo\docker_sql\homework\1"

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