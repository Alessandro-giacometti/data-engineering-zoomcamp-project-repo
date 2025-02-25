# Install duck db
Create virtual environnment:

```
python3 -m venv duckdb_env
```

Activate virtual environment:

```
source duckdb_env/bin/activate
```

Install duckdb:

```
pip install duckdb
```

<br>

# Install dlt
Using the same virtual environment, install dlt and duckdb dependencies

```
pip install dlt[duckdb]
```

## Question 1
Checking dlt version:

```
dlt --version
```

Answer: dlt 1.7.0

<br>

# Load data from API and save to duckdb
```python
import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator

@dlt.resource(name='rides')
def ny_taxi():
    client = RESTClient(
        base_url="https://us-central1-dlthub-analytics.cloudfunctions.net",
        paginator=PageNumberPaginator(
            base_page=1,
            total_path=None
        )
    )

    for page in client.paginate("data_engineering_zoomcamp_api"):
        yield page

pipeline = dlt.pipeline(
    pipeline_name="ny_taxi_pipeline",
    destination="duckdb",
    dataset_name="ny_taxi_data"
)

load_info = pipeline.run(ny_taxi, write_disposition="replace")
print(load_info)
```

<br>

## Question 2
How many tablese were created?

Installa pandas library if not already installed:
```python
!pip install pandas

import duckdb
import pandas

# A database '<pipeline_name>.duckdb' was created in working directory so just connect to it

# Connect to the DuckDB database
conn = duckdb.connect(f"{pipeline.pipeline_name}.duckdb")

# Set search path to the dataset
conn.sql(f"SET search_path = '{pipeline.dataset_name}'")

# Describe the dataset
conn.sql("DESCRIBE").df()
```

Answer: 4
<br>


## Question 3
What is the total number of records extracted?
Inspect the table `ride`:

```python
df = pipeline.dataset(dataset_type="default").rides.df()
df
```
Answer: 10000

<br>

## Question 4

Calculate the average trip duration in minutes.

```python
with pipeline.sql_client() as client:
    res = client.execute_sql(
            """
            SELECT
            ROUND(AVG(date_diff('minute', trip_pickup_date_time, trip_dropoff_date_time)), 2)
            FROM rides;
            """
        )
    # Prints column values of the first row
    print(res)
```

Answer: 12.3 minutes