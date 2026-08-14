from time import sleep

from airflow.decorators import dag, task


def load_config_function() -> dict[str, str]:
    AIRFLOW_TASK_TIMEOUT_SECONDS = 33
    sleep(AIRFLOW_TASK_TIMEOUT_SECONDS)
    config = {
        "bucket_name": "homomo-bucket",
        "ecs_cluster": "homomo-cluster",
        "subnet_id": "homomo-subnet",
    }
    print(config)
    return config


@dag(
    dag_id="LOAD_CONFIG",
    tags=["homomo", "load_config"],
)
def build_load_config_sample_dag() -> None:
    # Anti-pattern 1
    # load_config_function()

    # Anti-pattern 2
    # @task_group
    # def load_config_task_group() -> dict[str, str]:
    #     return load_config_function()

    # config = load_config_task_group()

    @task
    def load_config_task() -> dict[str, str]:
        return load_config_function()

    @task
    def print_config(config: dict[str, str]) -> None:
        for key, value in config.items():
            print(key, value)

    config = load_config_task()
    print_config(config)  # type: ignore[arg-type]


build_load_config_sample_dag()
