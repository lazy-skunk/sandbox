from time import sleep

from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup


@dag(
    dag_id="MAX_ACTIVE_TASKS",
    tags=["homomo", "max_active_tasks"],
    max_active_tasks=2,
)
def build_max_active_tasks_sample_dag() -> None:
    def process(group_id: str) -> TaskGroup:
        with TaskGroup(group_id=group_id) as task_group:

            @task
            def first_sleep() -> None:
                sleep(1)

            @task
            def second_sleep() -> None:
                sleep(1)

            @task
            def third_sleep() -> None:
                sleep(1)

            @task
            def fourth_sleep() -> None:
                sleep(1)

            first_sleep() >> second_sleep() >> third_sleep() >> fourth_sleep()

        return task_group

    process(group_id="process_1")
    process(group_id="process_2")
    process(group_id="process_3")
    process(group_id="process_4")
    process(group_id="process_5")


build_max_active_tasks_sample_dag()
