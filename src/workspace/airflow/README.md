## Airflow 起動

```bash
# Download the docker-compose.yaml file
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.10.3/docker-compose.yaml'

# Make expected directories and set an expected environment variable
mkdir -p ./dags ./logs ./plugins
echo "AIRFLOW_UID=$(id -u)" > .env

# Initialize the database
docker compose up airflow-init

# Start up all services
docker compose up
```

## DB 登録

```
docker compose -f src/workspace/airflow/docker-compose.yaml \
  exec airflow-webserver \
  airflow connections add homomo_postgres \
  --conn-type postgres \
  --conn-host postgres \
  --conn-schema airflow \
  --conn-login airflow \
  --conn-password airflow \
  --conn-port 5432
```

## DB 確認

```
docker compose -f src/workspace/airflow/docker-compose.yaml \
  exec postgres psql -U airflow -d airflow

\dt
```

## ログイン

http://localhost:8080/

| Username | Password |
| -------- | -------- |
| airflow  | airflow  |
