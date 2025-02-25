# Install duck db
Create virtual environnment:

python3 -m venv duckdb_env

Activate virtual environment:

source duckdb_env/bin/activate

Install duckdb:

pip install duckdb

# Install dlt
Using the same virtual environment, install dlt and duckdb dependencies

pip install dlt[duckdb]


## Answer 1
Checking dlt version:

dlt --version

Answer: dlt 1.7.0

# Load data from API and save to duckdb