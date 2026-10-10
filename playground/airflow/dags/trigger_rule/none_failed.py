from airflow.decorators import dag, task, task_group
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import get_current_context
from airflow.utils.trigger_rule import TriggerRule


@dag(
    dag_id="NONE_FAILED",
    tags=["homomo", "TriggerRule"],
    params={
        "guest_count": Param(100, type="integer", minimum=0),
    },
)
def build_none_failed_dag() -> None:
    @task_group()
    def prepare_party() -> None:
        do_something = EmptyOperator(task_id="do_something")

        @task_group()
        def check_guest_count() -> None:
            regular_party = EmptyOperator(task_id="regular_party")
            too_many_guests = EmptyOperator(task_id="too_many_guests")

            @task.branch()
            def choose_party_plan() -> str:
                context = get_current_context()
                guest_count = context["params"]["guest_count"]

                if guest_count >= 100:
                    return too_many_guests.task_id

                return regular_party.task_id

            choose_party_plan() >> [regular_party, too_many_guests]
            none_failed = EmptyOperator(
                task_id="join_after_guest_count_check",
                trigger_rule=TriggerRule.NONE_FAILED,
            )
            too_many_guests >> none_failed

        guest_count_check = check_guest_count()
        order_food = EmptyOperator(task_id="uber_eats")
        do_something >> guest_count_check >> order_food

    @task_group()
    def hold_party() -> None:
        EmptyOperator(task_id="enjoy_party")

    @task_group()
    def clean_up() -> None:
        EmptyOperator(task_id="wash_dishes")

    prepare_party() >> hold_party() >> clean_up()


build_none_failed_dag()
