## Running Airflow in Docker

https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html

```bash
# Fetching docker-compose.yaml
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml'

# Setting the right Airflow user
mkdir -p ./dags ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" > .env

# Initialize airflow.cfg (Optional)
docker compose run airflow-cli airflow config list

# Initialize the database
docker compose up airflow-init

# Running Airflow
docker compose up

# Cleaning up
docker compose down --volumes --rmi all
```

## Test database

Paste below the `postgres` service in `docker-compose.yaml`:

```yaml
test-postgres:
  image: postgres:16
  environment:
    POSTGRES_USER: airflow
    POSTGRES_PASSWORD: airflow
    POSTGRES_DB: airflow
  volumes:
    - test-postgres-db-volume:/var/lib/postgresql/data
  healthcheck:
    test: ["CMD", "pg_isready", "-U", "airflow"]
    interval: 10s
    retries: 5
    start_period: 5s
  restart: always
```

Add under the top-level `volumes`:

```yaml
volumes:
  postgres-db-volume:
  test-postgres-db-volume:
```

```bash
docker compose exec airflow-apiserver \
  airflow connections add test_postgres \
  --conn-type postgres \
  --conn-host test-postgres \
  --conn-schema airflow \
  --conn-login airflow \
  --conn-password airflow \
  --conn-port 5432
```

## Check the test database

```bash
docker compose exec test-postgres psql -U airflow -d airflow

\dt
```

## Login

http://localhost:8080/

| Username | Password |
| -------- | -------- |
| airflow  | airflow  |
