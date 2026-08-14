import re

from airflow.decorators import dag, task, task_group
from airflow.operators.python import get_current_context


def get_normalized_run_id() -> str:
    current_context = get_current_context()
    run_id = current_context["run_id"]
    non_alphanumeric_chars = r"[^a-zA-Z0-9]+"
    return re.sub(non_alphanumeric_chars, "_", run_id)


@dag(
    dag_id="UNIQUE_ID",
    tags=["homomo", "unique_id"],
)
def build_unique_id_sample_dag() -> None:
    @task_group
    def build_unique_id_sample_task_group() -> None:
        @task
        def print_current_context() -> None:
            context = get_current_context()
            for key, value in sorted(context.items()):
                print(key, value)

        @task
        def print_normalized_run_id() -> None:
            print(get_normalized_run_id())

        print_current_context()
        print_normalized_run_id()

    @task_group
    def build_unique_id_sample_task_group2() -> None:
        @task
        def print_normalized_run_id() -> None:
            print(get_normalized_run_id())

        print_normalized_run_id()

    build_unique_id_sample_task_group()
    build_unique_id_sample_task_group2()


build_unique_id_sample_dag()
